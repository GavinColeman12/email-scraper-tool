"""Page 6: Pattern learning + bounce analytics."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from src.bounce_tracker import (
    get_domain_pattern_stats, get_industry_pattern_stats,
    parse_gmail_bounces, init_bounce_tables,
)
from src.industry_patterns import INDUSTRY_PATTERN_PRIORS, get_patterns_for

st.set_page_config(page_title="Pattern Learning", page_icon="📊", layout="wide")
st.title("📊 Pattern Learning & Bounce Analytics")
st.caption(
    "Tracks which email patterns actually land vs bounce vs reply. "
    "As you send more emails, the system learns per-domain and per-industry."
)

init_bounce_tables()

tab_industry, tab_domain, tab_bounces = st.tabs([
    "Industry Performance", "Domain Performance", "Bounce Processing",
])

# ── Tab 1: Industry Performance ──────────────────────────────────────
with tab_industry:
    st.subheader("Per-industry pattern success")
    st.caption(
        "Left: the research-based priors we START with. Right: what YOUR "
        "actual sends reveal for this industry. Prefer the observed data "
        "once you have enough sample size."
    )

    industry = st.selectbox(
        "Industry", sorted(INDUSTRY_PATTERN_PRIORS.keys())
    )
    min_sends = st.slider("Min sample size to display", 1, 20, 3,
                           help="Filters out patterns with too few sends for meaningful data.")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Current Priors** (from research)")
        priors = get_patterns_for(industry)
        priors_df = pd.DataFrame(priors, columns=["pattern", "weight"])
        priors_df["weight"] = (priors_df["weight"] * 100).round(1).astype(str) + "%"
        st.dataframe(priors_df, use_container_width=True, hide_index=True)

    with col2:
        st.markdown("**Observed Performance** (your sends)")
        stats = get_industry_pattern_stats(industry, min_sends)
        if stats:
            df = pd.DataFrame(stats)
            df["delivery_rate"] = (df["delivery_rate"] * 100).round(1).astype(str) + "%"
            df["reply_rate"] = (df["reply_rate"] * 100).round(1).astype(str) + "%"
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info(
                f"Not enough send data yet for '{industry}'. "
                f"Need at least {min_sends} sends per pattern. "
                "Send some emails first, then come back."
            )

# ── Tab 2: Domain Performance ────────────────────────────────────────
with tab_domain:
    st.subheader("Per-domain pattern success")
    st.caption("For repeat domains, which specific patterns have worked.")
    domain = st.text_input(
        "Domain", placeholder="e.g. midtowndental.com"
    )
    if domain:
        stats = get_domain_pattern_stats(domain.strip().lower())
        if stats:
            df = pd.DataFrame(stats)
            df["delivery_rate"] = (df["delivery_rate"] * 100).round(1).astype(str) + "%"
            df["reply_rate"] = (df["reply_rate"] * 100).round(1).astype(str) + "%"
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info(f"No send history for `{domain}` yet.")

# ── Tab 3: Bounce Processing ─────────────────────────────────────────
with tab_bounces:
    st.subheader("Process Gmail Bounces")
    st.caption(
        "Fetches recent Mail Delivery Subsystem messages from your Gmail "
        "inbox and updates bounce tracking. Only matches emails that have "
        "already been logged via bounce_tracker.log_send() — so this "
        "works after you've sent some emails from the reputation-audit-tool."
    )

    lookback = st.slider("Lookback days", 1, 30, 7)

    if st.button("📬 Process bounces from Gmail", type="primary"):
        try:
            # Reuses Gmail OAuth from sister project — assumes creds exist
            # at ~/Downloads/reputation-audit-tool/credentials/ OR a compatible
            # token.json is wired into gmail_sender.py here.
            # Import is conditional so the rest of the page works without Gmail.
            import sys as _sys, os as _os
            audit_tool_src = "/Users/gavincoleman/Downloads/reputation-audit-tool"
            if _os.path.isdir(audit_tool_src) and audit_tool_src not in _sys.path:
                _sys.path.insert(0, audit_tool_src)
            try:
                from src.gmail_sender import _get_credentials  # from sister project
                from googleapiclient.discovery import build
                creds = _get_credentials()
                service = build("gmail", "v1", credentials=creds)
            except Exception as import_err:
                st.error(
                    f"Gmail service not available: {import_err}. "
                    "Make sure Gmail OAuth is set up in the reputation-audit-tool "
                    "project or add gmail_sender to this project."
                )
                st.stop()

            count = parse_gmail_bounces(service, lookback_days=lookback)
            st.success(f"✅ Processed {count} bounces from Gmail")
            if count == 0:
                st.info(
                    "0 bounces found — that's either because you haven't "
                    "sent anything yet OR nothing bounced in the lookback "
                    "window. Both good signals!"
                )
        except Exception as e:
            st.error(f"Error: {e}")

    st.divider()
    st.markdown(
        """
        ### How bounce tracking works

        1. Every email sent via the outreach pipeline is logged in `email_sends`
        with the pattern used (e.g. `first.last`, `first`, `flast`).
        2. Gmail sends bounce notifications from `mailer-daemon@googlemail.com`.
        3. When you click **Process bounces**, this page:
           - Fetches recent MDN messages
           - Parses the bounced email address + reason
           - Classifies the bounce (hard / soft / block)
           - Updates `email_sends.status = 'bounced'`
           - Increments `pattern_success` and `industry_pattern_success` bounce counts
        4. Reply tracking is similar: if a recipient replies to your send,
        `mark_reply()` is called and replies are tracked per-pattern.
        5. After enough sends, the **Industry Performance** tab above shows
        which patterns ACTUALLY work for your sending style + target industries,
        and you can update the priors in `src/industry_patterns.py` accordingly.
        """
    )
