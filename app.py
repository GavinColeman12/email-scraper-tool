"""Email Scraper — home-page dashboard."""
import pandas as pd
import streamlit as st

from src import storage
from src.dashboard_queries import (
    lifetime_kpis,
    daily_rollup,
    enriched_searches,
    industry_options,
)


st.set_page_config(
    page_title="Email Scraper",
    page_icon="📧",
    layout="wide",
)

storage.init_db()

st.title("📧 Email Scraper — Dashboard")
st.caption(
    "Fresh decision-maker emails from Google Maps + website scraping, "
    "with bounce-tracked feedback learning."
)

# ══════════════════════════════════════════════════════════════════════
# ROW 1 — Lifetime KPIs
# ══════════════════════════════════════════════════════════════════════

kpis = lifetime_kpis()

st.subheader("Lifetime metrics")
row1 = st.columns(4)
row1[0].metric(
    "Searches run",
    kpis.get("search_count", 0),
    help="Total Google Maps searches submitted.",
)
row1[1].metric(
    "Businesses found",
    f"{kpis.get('biz_count', 0):,}",
    help="Unique businesses discovered across all searches.",
)
row1[2].metric(
    "Emails found",
    f"{kpis.get('emails_found', 0):,}",
    help="Businesses where the pipeline produced a primary_email. "
         "Not every one is deliverable — see 'Safe to send' for that.",
)
row1[3].metric(
    "Decision makers identified",
    f"{kpis.get('dms_identified', 0):,}",
    help="Businesses with a DM name AND a founder/owner/CEO credential "
         "identified (from website, LinkedIn, or NPI).",
)

row2 = st.columns(4)
row2[0].metric(
    "Actionable leads 🎯",
    f"{kpis.get('actionable_leads', 0):,}",
    help="DM identified + NB-validated email + safe_to_send flag. "
         "This is the number you can send cold outreach to with confidence.",
)
row2[1].metric(
    "Emails sent",
    f"{kpis.get('total_sent', 0):,}",
    help="Sends logged in email_sends. Populated by the outreach side "
         "once you start sending.",
)
row2[2].metric(
    "Bounces",
    f"{kpis.get('total_bounced', 0):,}",
    delta=(f"-{kpis.get('bounce_rate', 0):.1f}% rate"
           if kpis.get("total_sent") else None),
    delta_color="inverse",
    help=f"{kpis.get('hard_bounces', 0)} hard bounces, "
         f"{(kpis.get('total_bounced', 0) - kpis.get('hard_bounces', 0))} soft.",
)
row2[3].metric(
    "Replies",
    f"{kpis.get('replies', 0):,}",
    delta=(f"+{kpis.get('reply_rate', 0):.1f}% rate"
           if kpis.get("total_sent") else None),
    help="Genuine replies detected via the bounce/reply tracker.",
)

st.divider()

# ══════════════════════════════════════════════════════════════════════
# ROW 2 — Trend chart + filters
# ══════════════════════════════════════════════════════════════════════

st.subheader("Trend — daily activity")

fc1, fc2 = st.columns([1, 3])
with fc1:
    window = st.selectbox(
        "Date range",
        options=[7, 14, 30, 60, 90, 180],
        format_func=lambda d: f"Last {d} days",
        index=2,  # default 30
    )
    agg = st.radio(
        "Aggregation",
        options=["Day", "Week"],
        horizontal=True,
        help="Weekly rolls up into 7-day sums — smoother trendlines.",
    )
with fc2:
    industries = industry_options()
    industry_filter = st.selectbox(
        "Industry filter",
        options=["(all)"] + industries,
        index=0,
        help="Substring match on business_type — e.g. 'dental' catches "
             "'dental clinic', 'dental office', 'dental practice'.",
    )

industry_arg = None if industry_filter == "(all)" else industry_filter
rollup = daily_rollup(days=int(window), industry=industry_arg)

# Build DataFrame for chart
df = pd.DataFrame(rollup)
if df.empty or df[["found", "sent", "bounces", "dms", "safe"]].sum().sum() == 0:
    st.info(
        "No activity in the selected window. "
        "Try a longer date range or clear the industry filter."
    )
else:
    df["day"] = pd.to_datetime(df["day"])
    if agg == "Week":
        df = (
            df.set_index("day")
            .resample("W-MON", label="left", closed="left")
            .sum()
            .reset_index()
        )
    df = df.set_index("day")

    # Two side-by-side charts — scrape activity vs send activity,
    # so the absolute scales don't fight each other (emails_found
    # can be 10x larger than emails_sent in early days).
    chart_cols = st.columns(2)

    with chart_cols[0]:
        st.caption("**📥 Scraping activity**")
        scrape_df = df[["found", "dms", "safe"]].rename(columns={
            "found": "Emails found",
            "dms": "DMs identified",
            "safe": "Safe to send",
        })
        st.line_chart(scrape_df, height=280)

    with chart_cols[1]:
        st.caption("**📤 Outreach activity**")
        send_df = df[["sent", "bounces", "replies"]].rename(columns={
            "sent": "Sent",
            "bounces": "Bounced",
            "replies": "Replies",
        })
        # If zero sends across the window, show a hint instead of a
        # flat-zero chart.
        if send_df.sum().sum() == 0:
            st.caption(
                "_No sends logged yet in this window. Once you start "
                "sending, rows flow into `email_sends` and this chart "
                "tracks bounce + reply rates over time._"
            )
            st.line_chart(send_df, height=280)
        else:
            st.line_chart(send_df, height=280)

st.divider()

# ══════════════════════════════════════════════════════════════════════
# ROW 3 — Per-search table (enriched)
# ══════════════════════════════════════════════════════════════════════

st.subheader("Recent searches")

searches = enriched_searches(limit=50)
if not searches:
    st.info("No searches yet. Head to **🔎 Find Businesses** to start.")
else:
    # Format rows for display
    table = []
    for s in searches:
        sent = s.get("sent", 0) or 0
        bounced = s.get("bounced", 0) or 0
        biz = s.get("biz_count", 0) or 0
        found = s.get("emails_found", 0) or 0
        dms = s.get("dms_identified", 0) or 0
        scraped = s.get("scraped", 0) or 0
        safe = s.get("safe_to_send", 0) or 0
        table.append({
            "#": s["id"],
            "Query": s["query"],
            "Location": s.get("location") or "",
            "Created": str(s.get("created_at") or "")[:10],
            "Businesses": biz,
            "Scraped": scraped,
            "Emails": found,
            "DMs": dms,
            "Safe to send": safe,
            "Sent": sent,
            "Bounced": bounced,
            "Bounce %": f"{s.get('bounce_rate', 0):.1f}%" if sent else "—",
            "Replies": s.get("replies", 0) or 0,
        })
    st.dataframe(
        pd.DataFrame(table),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Businesses": st.column_config.ProgressColumn(
                "Businesses", min_value=0,
                max_value=max([r["Businesses"] for r in table] + [1]),
                format="%d",
            ),
            "Emails": st.column_config.ProgressColumn(
                "Emails", min_value=0,
                max_value=max([r["Emails"] for r in table] + [1]),
                format="%d",
            ),
            "Safe to send": st.column_config.NumberColumn(
                "Safe to send",
                help="NB-verified deliverable emails.",
            ),
            "Bounce %": st.column_config.TextColumn(
                "Bounce %",
                help="Bounces ÷ sends. '—' when no sends logged yet.",
            ),
        },
    )

    # Delete buttons in a compact row below
    with st.expander("🗑️  Delete a search"):
        for s in searches[:20]:
            del_col1, del_col2 = st.columns([4, 1])
            del_col1.caption(f"#{s['id']} — {s['query']} · "
                              f"{s.get('biz_count', 0)} biz")
            if del_col2.button("Delete", key=f"del_{s['id']}"):
                storage.delete_search(s["id"])
                st.rerun()

st.divider()

# ══════════════════════════════════════════════════════════════════════
# Workflow intro (moved below the dashboard)
# ══════════════════════════════════════════════════════════════════════

with st.expander("📖 Workflow + how this tool works"):
    st.markdown("""
### Workflow

1. **🔎 Find Businesses** — Search Google Maps for businesses by type + location (e.g. *"dental clinics Manhattan"*). Synonym fan-out means "Law" expands to 9 variants automatically.
2. **🚀 Bulk Scrape** — Run 200+ businesses in the background. Pick **Volume mode** (recommended) for cheap mass outreach, or **Triangulation** for high-stakes targets.
3. **📥 Export CSV** — Download a 23-column CSV ready for import into the reputation audit tool or your outreach platform.
4. **✅ Re-verify Existing Emails** (optional) — NB re-check for stale rows; Bulk Scrape already verifies in-pipeline.
5. **📊 Pattern Learning** — Bounce analytics feed back into future runs.
6. **🔁 Replay** — Re-run past searches with new logic to A/B your fixes at ~$0 cost.

### Confidence tiers (Volume mode)

| Badge | Meaning | Safe to send? |
|---|---|---|
| 🟢 HIGH (volume_verified) | NB returned VALID | Yes — deliverable confirmed |
| 🟣 REVIEW (volume_review) | NB returned UNKNOWN | No — human glance first |
| 🟡 MEDIUM (volume_scraped) | Catchall domain or untested | Risky |
| 🔴 LOW (volume_guess) | Industry-prior, not NB-verified | Spray at your own risk |
| ⚫ EMPTY (volume_empty) | Pipeline found nothing deliverable | Skip |

Generic inboxes — info@, contact@, hello@, smile@, etc. — are **never** picked. Decision makers first; industry prior is the last resort.
    """)
