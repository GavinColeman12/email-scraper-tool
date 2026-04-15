import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import streamlit as st

from src import storage
from src.email_verifier import verify_email, STATUS_VALID, STATUS_INVALID, STATUS_UNKNOWN
from src.secrets import get_secret

st.set_page_config(page_title="Verify Deliverability", page_icon="✅", layout="wide")
st.title("✅ Verify Email Deliverability")

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

zb_key_present = bool(get_secret("ZEROBOUNCE_API_KEY"))
mode_options = ["free_mx_only"]
if zb_key_present:
    mode_options.extend(["paid_only", "hybrid"])

mode = st.radio(
    "Mode",
    mode_options,
    format_func=lambda k: {
        "free_mx_only": "🆓 Free — MX record check only (fast, catches ~80% of dead emails)",
        "hybrid": "💰 Hybrid — MX first, ZeroBounce only for MX-passing (recommended if paid)",
        "paid_only": "💰 Paid only — ZeroBounce for every email (most accurate, most expensive)",
    }[k],
    horizontal=False,
)

if not zb_key_present:
    st.caption(
        "💡 To enable paid ZeroBounce verification (~$0.007/email, ~95% accuracy), "
        "add `ZEROBOUNCE_API_KEY` to your `.env` file."
    )

paid_check = mode in ("paid_only", "hybrid")

if mode == "paid_only":
    est_cost = len(to_verify) * 0.007
    st.caption(f"💰 Estimated ZeroBounce cost: ${est_cost:.2f}")
elif mode == "hybrid":
    # Only ~60-80% pass MX check, so ZB cost is lower
    est_cost = len(to_verify) * 0.007 * 0.75
    st.caption(f"💰 Estimated ZeroBounce cost: ~${est_cost:.2f} (only MX-passing emails get paid verification)")

# ── Run verification ──
if st.button(f"▶️ Verify {len(to_verify)} emails",
              disabled=not to_verify, type="primary"):
    progress = st.progress(0)
    status_box = st.empty()

    def _verify_one(biz):
        email = biz.get("primary_email", "")
        if not email:
            return biz, {"status": STATUS_UNKNOWN, "reason": "no email"}
        result = verify_email(email, paid_check=paid_check)
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
