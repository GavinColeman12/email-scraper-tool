import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import streamlit as st

from src import storage
from src.email_verifier import (
    verify_email, verify_smtp, verify_full,
    STATUS_VALID, STATUS_INVALID, STATUS_UNKNOWN,
)
from src.secrets import get_secret

st.set_page_config(page_title="Re-verify Emails", page_icon="✅", layout="wide")
st.title("✅ Re-verify Existing Emails (optional)")
st.warning(
    "⚠️ **Bulk Scrape already verifies emails in-pipeline via NeverBounce.** "
    "Only use this page to (a) re-verify stale results months later, "
    "(b) verify emails added manually, or (c) cross-check a suspicious send. "
    "Running it on fresh Bulk Scrape output re-spends credits for no new signal."
)

storage.init_db()

searches = storage.list_searches()
if not searches:
    st.warning("No searches yet. Go to **🔎 Find Businesses** first.")
    st.stop()

labels = {s["id"]: f"#{s['id']} — {s['query']}"
          for s in searches}
search_id = st.selectbox("Search", options=list(labels.keys()),
                          format_func=lambda k: labels[k])

businesses = storage.list_businesses(search_id=search_id, has_email=True)
to_verify = [b for b in businesses if not b.get("email_status") or b.get("email_status") == "unknown"]

c1, c2, c3 = st.columns(3)
c1.metric("Businesses with email", len(businesses))
c2.metric("Pending verification", len(to_verify))
c3.metric("Already verified", len(businesses) - len(to_verify))

# ── Verification mode ──
st.subheader("Verification mode")

try:
    zb_key_present = bool(get_secret("ZEROBOUNCE_API_KEY"))
except Exception:
    zb_key_present = False

try:
    from src.neverbounce import is_available as nb_available, get_account_info
    nb_key_present = nb_available()
except Exception:
    nb_key_present = False

mode_options = ["free_mx_only", "free_smtp"]
if nb_key_present:
    mode_options.append("neverbounce")
if zb_key_present:
    mode_options.extend(["paid_only", "hybrid"])

default_index = 2 if nb_key_present else 1  # prefer NeverBounce when available

mode = st.radio(
    "Mode",
    mode_options,
    format_func=lambda k: {
        "free_mx_only": "🆓 MX only — checks domain exists (fast, catches dead domains)",
        "free_smtp": "🆓 MX + SMTP — checks the actual MAILBOX exists (slower but catches bounces)",
        "neverbounce": "✅ NeverBounce (recommended) — MX + SMTP + NeverBounce authoritative check (~$0.003/email, 1000 free/month)",
        "hybrid": "💰 Hybrid — MX + SMTP + ZeroBounce for remaining unknowns",
        "paid_only": "💰 Paid only — ZeroBounce for every email",
    }[k],
    horizontal=False,
    index=default_index,
)

if mode == "free_smtp":
    st.caption(
        "SMTP verification connects to the mail server and checks if the "
        "specific mailbox exists. Catches 'address not found' bounces. "
        "Free but slower (~5 sec/email). Also detects catch-all domains."
    )
elif mode == "neverbounce":
    st.caption(
        "✅ **Most accurate** — NeverBounce provides authoritative deliverability "
        "signal including catch-all detection. Free tier: 1,000 verifications/month."
    )
    # Show remaining credits
    try:
        info = get_account_info()
        if info and not info.get("error"):
            remaining = info.get("credits_info", {}).get("free_credits_remaining", "?")
            st.caption(f"🎫 NeverBounce free credits remaining: {remaining}")
    except Exception:
        pass

if not nb_key_present:
    st.caption(
        "💡 To enable NeverBounce (recommended — 1,000 free/month), "
        "add `NEVERBOUNCE_API_KEY` to your `.env` or Streamlit Cloud secrets."
    )

paid_check = mode in ("paid_only", "hybrid")
smtp_check = mode in ("free_smtp", "hybrid", "neverbounce")
nb_check = mode == "neverbounce"

if mode == "paid_only":
    est_cost = len(to_verify) * 0.007
    st.caption(f"💰 Estimated ZeroBounce cost: ${est_cost:.2f}")
elif mode == "hybrid":
    est_cost = len(to_verify) * 0.007 * 0.75
    st.caption(f"💰 Estimated ZeroBounce cost: ~${est_cost:.2f} (only MX-passing emails get paid verification)")
elif mode == "neverbounce":
    est_cost = len(to_verify) * 0.003
    st.caption(f"💰 NeverBounce cost after free tier: ~${est_cost:.2f} (first 1,000/month free)")

# ── Run verification ──
if st.button(f"▶️ Verify {len(to_verify)} emails",
              disabled=not to_verify, type="primary"):
    progress = st.progress(0)
    status_box = st.empty()

    def _verify_one(biz):
        email = biz.get("primary_email", "")
        if not email:
            return biz, {"status": STATUS_UNKNOWN, "reason": "no email"}

        if nb_check:
            # Full waterfall: MX → SMTP → NeverBounce
            result = verify_full(
                email, try_smtp=True, try_paid=False, try_neverbounce=True,
            )
            # Flatten reasons list into a single string for storage
            if isinstance(result.get("reasons"), list):
                result["reason"] = " | ".join(result["reasons"])
        else:
            # Step 1: MX check (always, fast)
            result = verify_email(email, paid_check=paid_check)

            # Step 2: SMTP mailbox check (if enabled and MX passed)
            if smtp_check and result.get("status") == STATUS_VALID:
                smtp_result = verify_smtp(email, timeout=10)
                if smtp_result["status"] == STATUS_INVALID:
                    # Mailbox doesn't exist — override MX result
                    result = smtp_result
                elif smtp_result["status"] == "risky":
                    result["status"] = "risky"
                    result["reason"] = smtp_result["reason"]

        storage.update_business_verification(
            biz["id"], result.get("status", STATUS_UNKNOWN),
            result.get("reason", ""),
        )
        return biz, result

    completed = 0
    valid_count = 0
    invalid_count = 0
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(_verify_one, b) for b in to_verify]
        for fut in as_completed(futures):
            try:
                biz, result = fut.result()
            except Exception as e:
                biz, result = {"business_name": "?", "primary_email": ""}, {"status": STATUS_UNKNOWN}
                st.write(f"⚠️ {biz.get('business_name')}: {e}")
            completed += 1
            status = result.get("status", STATUS_UNKNOWN)
            if status == STATUS_VALID:
                valid_count += 1
            elif status == STATUS_INVALID:
                invalid_count += 1
            progress.progress(completed / len(to_verify))
            status_box.write(
                f"**{completed}/{len(to_verify)}** · {biz.get('business_name')} → "
                f"{biz.get('primary_email')} → **{status}** ({result.get('reason', '')})"
            )

    progress.empty()
    status_box.success(
        f"✅ Done — {valid_count} valid, {invalid_count} invalid, "
        f"{completed - valid_count - invalid_count} unknown"
    )
    st.rerun()

# ── Results ──
st.divider()
st.subheader("Verification results")

all_verified = [b for b in businesses if b.get("email_status")]
if not all_verified:
    st.info("No verified emails yet.")
else:
    status_filter = st.radio(
        "Show",
        ["all", "valid", "invalid", "risky", "unknown"],
        horizontal=True,
    )

    def matches(b):
        if status_filter == "all":
            return True
        return b.get("email_status") == status_filter

    filtered = [b for b in all_verified if matches(b)]

    status_emoji = {
        "valid": "✅",
        "invalid": "❌",
        "risky": "⚠️",
        "unknown": "❓",
        "disposable": "🗑️",
    }

    rows = []
    for b in filtered:
        s = b.get("email_status", "unknown")
        rows.append({
            "Status": f"{status_emoji.get(s, '')} {s}",
            "Business": b["business_name"],
            "Email": b.get("primary_email", ""),
            "Reason": b.get("email_verification_reason", ""),
        })
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
