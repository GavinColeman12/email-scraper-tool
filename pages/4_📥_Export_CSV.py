import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import io

import pandas as pd
import streamlit as st

from src import storage

st.set_page_config(page_title="Export CSV", page_icon="📥", layout="wide")
st.title("📥 Export to CSV")
st.caption(
    "Download scraped businesses as a CSV formatted for import into the "
    "reputation audit tool's **📥 Import Prospects** page."
)

storage.init_db()

searches = storage.list_searches()
if not searches:
    st.warning("No searches yet.")
    st.stop()

from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from src.email_verifier import (
    verify_smtp, verify_smtp_patterns, verify_mx,
    STATUS_VALID, STATUS_INVALID, STATUS_UNKNOWN,
)


# Title prefixes to strip when parsing names (case-insensitive)
_TITLE_PREFIXES = {
    "dr", "dr.", "doctor", "mr", "mr.", "mrs", "mrs.", "ms", "ms.",
    "miss", "prof", "prof.", "professor", "attorney", "atty", "atty.",
    "sir", "madam", "rev", "rev.", "reverend", "hon", "hon.",
    "honorable", "capt", "capt.", "captain", "lt", "lt.",
}

# Credential suffixes to strip (case-insensitive, applied after comma split)
_CREDENTIAL_SUFFIXES = {
    "dmd", "dds", "md", "do", "phd", "dpm", "od", "dc", "dvm", "edd",
    "mba", "jd", "esq", "esquire", "cpa", "rn", "np", "pa", "pa-c",
    "bsn", "msn", "aprn", "fnp", "mph", "mha", "ms", "ma", "ba", "bs",
    "facp", "faap", "facog", "facs",
}


def split_contact_name(full_name: str) -> tuple:
    """
    Parse a contact name into (first, last) by stripping prefix titles
    (Dr., Attorney, etc.) and suffix credentials (DMD, Esq., PhD, etc.).

    Examples:
      'Dr. Caleb Martin, DMD'  → ('Caleb', 'Martin')
      'Linda Miller'           → ('Linda', 'Miller')
      'Sarah Chen, MD, MPH'    → ('Sarah', 'Chen')
      'J. Robert Smith'        → ('J.', 'Smith')   # middle initial lost OK
      ''                       → ('', '')
    """
    if not full_name:
        return ("", "")

    # Strip credentials after comma: "Dr. Caleb Martin, DMD" → "Dr. Caleb Martin"
    name = full_name.split(",")[0].strip()

    # Tokenize
    tokens = name.split()
    if not tokens:
        return ("", "")

    # Strip prefix titles (possibly multiple, e.g. "Dr. Rev. Smith")
    while tokens and tokens[0].lower().rstrip(".") in _TITLE_PREFIXES:
        tokens.pop(0)

    # Strip suffix credentials that weren't comma-separated ("Smith DMD")
    while tokens and tokens[-1].lower().rstrip(".") in _CREDENTIAL_SUFFIXES:
        tokens.pop()

    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return (tokens[0], "")

    # First = first token, Last = last token (middle names ignored for email purposes)
    first = tokens[0]
    last = tokens[-1]
    return (first, last)

labels = {s["id"]: f"#{s['id']} — {s['query']}" for s in searches}
search_id = st.selectbox("Search", options=list(labels.keys()),
                          format_func=lambda k: labels[k])

# ── Filters ──
st.subheader("Filters")

col1, col2, col3 = st.columns(3)
only_verified = col1.checkbox("Only include VALID (MX-verified) emails", value=False)
only_with_email = col2.checkbox("Only include businesses with an email", value=True)
exclude_generic = col3.checkbox("Exclude generic emails (info@, contact@)", value=False)

min_rating = col1.number_input("Min rating", 0.0, 5.0, 0.0, step=0.1)
max_rating = col2.number_input("Max rating", 0.0, 5.0, 5.0, step=0.1)
min_reviews = col3.number_input("Min reviews", 0, 100000, 0)

# Confidence filter
st.markdown("**Confidence level** (decision-maker quality)")
conf_c1, conf_c2, conf_c3, conf_c4 = st.columns(4)
conf_high = conf_c1.checkbox("🟢 High", value=True)
conf_medium = conf_c2.checkbox("🟡 Medium", value=True)
conf_low = conf_c3.checkbox("🔴 Low", value=True)
conf_blank = conf_c4.checkbox("⚪ Blank (older data)", value=True)

allowed_confidences = set()
if conf_high: allowed_confidences.add("high")
if conf_medium: allowed_confidences.add("medium")
if conf_low: allowed_confidences.add("low")
if conf_blank: allowed_confidences.add("")

# ── Build CSV ──
businesses = storage.list_businesses(search_id=search_id)

GENERIC_PREFIXES = {"info", "contact", "hello", "office", "sales", "support",
                    "admin", "help", "inquiries", "feedback"}


def is_generic(email):
    if not email:
        return True
    prefix = email.split("@")[0].lower().split(".")[0]
    return prefix in GENERIC_PREFIXES


def matches(b):
    if only_with_email and not b.get("primary_email"):
        return False
    if only_verified and b.get("email_status") != "valid":
        return False
    if exclude_generic and is_generic(b.get("primary_email", "")):
        return False
    rating = b.get("rating") or 0
    if rating < min_rating or rating > max_rating:
        return False
    if (b.get("review_count") or 0) < min_reviews:
        return False
    if (b.get("confidence") or "") not in allowed_confidences:
        return False
    return True


filtered = [b for b in businesses if matches(b)]

c1, c2 = st.columns(2)
c1.metric("Total in search", len(businesses))
c2.metric("After filters", len(filtered))

# ── Pre-flight: re-verify every primary email NOW (reduces bounces to ~0) ──
st.divider()
st.subheader("✅ Pre-flight: verify emails before export")
st.caption(
    "Runs SMTP verification against the recipient mail server on every primary email "
    "that hasn't been verified yet. Anything that bounces will be marked INVALID and "
    "dropped from the export. **Do this before sending a batch.**"
)

to_verify = [b for b in filtered
             if b.get("primary_email")
             and b.get("email_status") not in ("valid", "invalid")]

ver_c1, ver_c2 = st.columns([3, 2])
with ver_c1:
    strict_mode = st.checkbox(
        "Strict mode: drop UNKNOWN results (server-blocked probes) as well as INVALID",
        value=False,
        help="Unchecked: UNKNOWN stays in the list (may bounce). "
             "Checked: only SMTP-VERIFIED emails survive (zero-bounce guarantee, "
             "but you lose ~15-20% of leads whose servers block verification).",
    )
with ver_c2:
    if st.button(f"🔄 Verify {len(to_verify)} pending emails",
                  disabled=not to_verify, type="primary"):
        prog = st.progress(0)
        status_box = st.empty()
        counts = {"valid": 0, "invalid": 0, "unknown": 0}

        def _verify_one(biz):
            email = biz.get("primary_email", "")
            if not email:
                return biz, None
            try:
                result = verify_smtp(email, timeout=8)
            except Exception as e:
                result = {"status": "unknown", "reason": str(e)}
            storage.update_business_verification(
                biz["id"], result.get("status", "unknown"),
                result.get("reason", ""),
            )
            return biz, result

        completed = 0
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = [ex.submit(_verify_one, b) for b in to_verify]
            for fut in as_completed(futures):
                biz, result = fut.result()
                completed += 1
                if result:
                    status = result.get("status", "unknown")
                    counts[status if status in counts else "unknown"] += 1
                    prog.progress(completed / len(to_verify))
                    status_box.write(
                        f"**{completed}/{len(to_verify)}** · {biz.get('business_name')} → "
                        f"{biz.get('primary_email')} → **{status}**"
                    )

        prog.empty()
        status_box.success(
            f"✅ Verified {completed} emails — "
            f"{counts['valid']} valid · {counts['invalid']} invalid · "
            f"{counts['unknown']} unknown (server blocked)"
        )
        st.rerun()

# Apply verification filter based on strict mode
if strict_mode:
    filtered = [b for b in filtered if b.get("email_status") == "valid"]
    st.info(f"🔒 Strict mode: showing only {len(filtered)} SMTP-VERIFIED leads")
else:
    # Exclude only invalid — keep valid + unknown + unverified
    filtered = [b for b in filtered if b.get("email_status") != "invalid"]

st.divider()

# ── Preview + export ──
if filtered:
    st.subheader("Preview")
    show_evidence = st.toggle(
        "Show verification evidence columns (SMTP / WHOIS / NPI / Pattern)",
        value=False,
        help="These tick columns derive from the triangulation evidence trail. "
             "Hidden by default to keep the preview readable.",
    )

    # Verification badge: combines confidence + SMTP + email_source for quick UI
    # Demotes 🟢 HIGH to 🟡 MEDIUM when the row is actually unsafe — i.e.
    # catch-all domain (deliverable-looking but no mailbox guarantee) or
    # triangulation marked email_safe_to_send=false. Keeps the badge
    # honest so the operator doesn't ship unverified sends.
    def _verify_badge(biz):
        conf = biz.get("confidence", "") or ""
        src = biz.get("email_source", "") or ""
        reason = (biz.get("email_verification_reason") or "").lower()
        safe_flag = biz.get("email_safe_to_send")
        is_unsafe = (safe_flag == 0 or safe_flag is False) or "catch-all" in reason
        if conf == "high" and "smtp_verified" in src:
            return "🟢 VERIFIED" if not is_unsafe else "🟡 MED (catch-all)"
        if conf == "high":
            return "🟢 HIGH" if not is_unsafe else "🟡 MED (catch-all)"
        if conf == "medium" and "unverified_with_signal" in src:
            return "🟡 MED (unverified)"
        if conf == "medium":
            return "🟡 MEDIUM"
        if conf == "skip":
            return "⛔ SKIP"
        if conf == "low":
            return "🔴 LOW"
        return "❔ ?"

    export_rows = []
    for b in filtered:
        email = b.get("primary_email", "")
        contact_name = b.get("contact_name", "")
        first_name, last_name = split_contact_name(contact_name)
        source = (b.get("email_source") or "").lower()
        # Derive the verification evidence from source suffixes + fields.
        # Triangulation rows set source="triangulation" and store agent
        # successes in professional_ids JSON, so we consult both paths.
        prof_obj = b.get("professional_ids") or {}
        if isinstance(prof_obj, str):
            try:
                import json as _json
                prof_obj = _json.loads(prof_obj)
            except Exception:
                prof_obj = {}
        agents_ok = set((prof_obj.get("agents_succeeded") or []) if isinstance(prof_obj, dict) else [])
        pattern_confirmed = bool(prof_obj.get("detected_pattern")) if isinstance(prof_obj, dict) else False

        smtp_ver = "✓" if ("smtp_verified" in source or "smtp_probe" in agents_ok
                           or "neverbounce" in agents_ok) else ""
        whois_ver = "✓" if ("whois_confirmed" in source or "whois" in agents_ok) else (
            "✗" if "whois_mismatch" in source else ""
        )
        npi_ver = "✓" if (
            "npi_registry" in source
            or "_pattern_confirmed" in source
            or "_npi_anchored" in source
            or "npi_healthcare" in agents_ok
            or pattern_confirmed
        ) else ""
        # Carry the triangulation evidence trail into the CSV so the audit
        # tool preserves it instead of discarding $5/business of work.
        prof_ids = b.get("professional_ids") or ""
        if not isinstance(prof_ids, str):
            try:
                import json as _json
                prof_ids = _json.dumps(prof_ids)
            except Exception:
                prof_ids = ""
        row = {
            "Badge": _verify_badge(b),
            "Score": b.get("lead_quality_score", ""),
            "Tier": b.get("lead_tier", ""),
        }
        if show_evidence:
            row["SMTP ✓"] = smtp_ver
            row["WHOIS ✓"] = whois_ver
            row["NPI/Pattern ✓"] = npi_ver
        row.update({
            "Business Name": b.get("business_name", ""),
            "Business Type": b.get("business_type", ""),
            "Location": b.get("address", ""),
            "Phone": b.get("phone", ""),
            "Website": b.get("website", ""),
            "Email": email,
            "First Name": first_name,
            "Last Name": last_name,
            "Contact Name": contact_name,
            "Contact Title": b.get("contact_title", ""),
            "Email Source": b.get("email_source", ""),
            "Confidence": b.get("confidence", ""),
            "Rating": b.get("rating", ""),
            "Review Count": b.get("review_count", ""),
            "Place ID": b.get("place_id", ""),
            "Email Status": b.get("email_status", ""),
            # Evidence trail — the audit tool reads this JSON to skip
            # rework and preserve triangulation context for email gen.
            "Professional IDs": prof_ids,
        })
        export_rows.append(row)

    df = pd.DataFrame(export_rows)
    # Sort by lead quality score DESC
    if "Score" in df.columns and df["Score"].dtype != object:
        df = df.sort_values("Score", ascending=False, na_position="last")

    st.dataframe(
        df.head(50), use_container_width=True, hide_index=True,
        column_config={
            "Website": st.column_config.LinkColumn("Website"),
            "Score": st.column_config.ProgressColumn(
                "Score", min_value=0, max_value=100, format="%d"
            ) if df["Score"].dtype != object else None,
        },
    )
    if len(df) > 50:
        st.caption(f"Showing top 50 of {len(df)} rows (full CSV download below)")

    # ── Export buttons ──
    st.divider()

    # Generic CSV (matches audit tool's "generic" import format)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    st.download_button(
        f"📥 Download {len(df)} prospects (generic CSV)",
        data=buf.getvalue(),
        file_name=f"scraped_prospects_search_{search_id}.csv",
        mime="text/csv",
    )

    # Apollo-format CSV was removed — it silently dropped the
    # triangulation evidence (Place ID, Professional IDs, Score/Tier,
    # Confidence, Email Source/Status), losing ~$5/business of enrichment
    # work. The generic CSV above carries everything the audit tool needs.

    st.info(
        "💡 In the reputation audit tool, go to **📥 Import Prospects → Upload CSV** "
        "and drop the generic CSV in. All the scraper's evidence (triangulation "
        "trail, place_id, rating) comes across so the audit tool skips its own "
        "enrichment step."
    )
else:
    st.info("No businesses match the current filters.")
