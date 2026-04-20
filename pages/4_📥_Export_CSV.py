import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import io

import pandas as pd
import streamlit as st

from src import storage
from src.export_rows import build_rows, EXPORT_COLUMNS, verify_badge, split_contact_name

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


# split_contact_name + verify_badge now live in src/export_rows.py so this
# page and the Bulk Scrape page share the same schema. Imported above.

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

    # Delegated to src.export_rows so this page and the Bulk Scrape page
    # produce identical CSVs. Badge logic, name parsing, and evidence-
    # trail derivation all live in one module; see src/export_rows.py.
    df = pd.DataFrame(build_rows(filtered, include_evidence=show_evidence))
    # Enforce canonical column order for audit-tool compatibility
    _ordered = [c for c in EXPORT_COLUMNS if c in df.columns]
    _extra = [c for c in df.columns if c not in _ordered]
    df = df[_ordered + _extra]
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
