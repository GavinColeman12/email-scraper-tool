"""Email Scraper — home-page dashboard."""
import pandas as pd
import streamlit as st

from src import storage
from src.dashboard_queries import (
    lifetime_kpis,
    daily_rollup,
    daily_rollup_by_vertical,
    enriched_searches,
    industry_options,
    outreach_by_location,
    normalize_vertical,
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
# ROW 0 — Gmail sync control
# ══════════════════════════════════════════════════════════════════════
# Dashboard reads from `email_sends` — real outreach lives in Gmail
# until we pull it in. This button scans the last N days of Gmail and
# upserts into email_sends (outreach threads) + marks bounces
# (mailer-daemon threads).
sync_col, sync_msg = st.columns([1, 3])
with sync_col:
    sync_days = st.selectbox(
        "Gmail sync window", [7, 14, 30, 60, 90], index=2,
        format_func=lambda d: f"Last {d} days",
        key="_sync_days",
    )
    clicked = st.button("🔄 Sync sends & bounces from Gmail",
                         help="Scans Gmail for outreach + mailer-daemon bounces "
                              "and logs them to email_sends. Safe to re-run — "
                              "dedupe's by (email, sent_minute).")
with sync_msg:
    if clicked:
        st.info(
            "ℹ️ Gmail sync runs via the Gmail MCP integration. If your "
            "environment doesn't have the MCP Gmail tool configured, "
            "run the sync from your Claude Code session instead. "
            "The DB table `email_sends` is the single source of truth "
            "for the dashboard."
        )

st.divider()

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
    help="DM identified + NB-validated email + safe_to_send flag.",
)
row2[1].metric(
    "Emails sent",
    f"{kpis.get('total_sent', 0):,}",
    help="Sends logged in email_sends. Use the 'Sync from Gmail' button "
         "above to backfill from your outbox.",
)
row2[2].metric(
    "Bounces",
    f"{kpis.get('total_bounced', 0):,}",
    delta=(f"{kpis.get('bounce_rate', 0):.1f}% rate"
           if kpis.get("total_sent") else None),
    delta_color="inverse",
    help=f"{kpis.get('hard_bounces', 0)} hard bounces, "
         f"{(kpis.get('total_bounced', 0) - kpis.get('hard_bounces', 0))} soft.",
)
row2[3].metric(
    "Replies",
    f"{kpis.get('replies', 0):,}",
    delta=(f"{kpis.get('reply_rate', 0):.1f}% rate"
           if kpis.get("total_sent") else None),
    help="Genuine replies detected via the bounce/reply tracker.",
)

st.divider()

# ══════════════════════════════════════════════════════════════════════
# ROW 2 — Trend chart (single filter view)
# ══════════════════════════════════════════════════════════════════════

st.subheader("Trend — daily activity")

fc1, fc2 = st.columns([1, 3])
with fc1:
    window = st.selectbox(
        "Date range",
        options=[7, 14, 30, 60, 90, 180],
        format_func=lambda d: f"Last {d} days",
        index=2,
    )
    agg = st.radio(
        "Aggregation",
        options=["Day", "Week"],
        horizontal=True,
    )
with fc2:
    industries = industry_options()
    industry_filter = st.selectbox(
        "Industry filter (macro vertical)",
        options=["(all)"] + industries,
        index=0,
        help="Macro verticals — 'Law / Legal' rolls up attorney + law firm + "
             "bankruptcy attorney + litigator + etc.",
    )

industry_arg = None if industry_filter == "(all)" else industry_filter
rollup = daily_rollup(days=int(window), industry=industry_arg)

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
        if send_df.sum().sum() == 0:
            st.caption(
                "_No sends logged yet in this window. Hit the "
                "🔄 Sync button above to pull from Gmail._"
            )
        st.line_chart(send_df, height=280)

st.divider()

# ══════════════════════════════════════════════════════════════════════
# ROW 3 — Multi-industry breakdown (all verticals, one line each)
# ══════════════════════════════════════════════════════════════════════

st.subheader("Breakdown by industry")
st.caption(
    "Same two charts, but with **one line per macro vertical** so you "
    "can see which categories are driving the totals. Legend is clickable."
)

vert = daily_rollup_by_vertical(days=int(window))
if vert.get("scrape") or vert.get("outreach"):
    # Aggregate to match the Day/Week toggle above
    def _pivot(series: dict, days: list[str]) -> pd.DataFrame:
        if not series:
            return pd.DataFrame(index=pd.to_datetime(days))
        df_v = pd.DataFrame(series, index=pd.to_datetime(days))
        if agg == "Week":
            df_v = (df_v.resample("W-MON", label="left", closed="left").sum())
        # Drop verticals that are all-zero in the window — keeps legend clean
        return df_v.loc[:, df_v.sum(axis=0) > 0]

    scrape_v = _pivot(vert["scrape"], vert["days"])
    outreach_v = _pivot(vert["outreach"], vert["days"])

    breakdown_cols = st.columns(2)
    with breakdown_cols[0]:
        st.caption("**📥 Emails found by vertical**")
        if scrape_v.empty:
            st.caption("_No scraping activity in window._")
        else:
            st.line_chart(scrape_v, height=320)
    with breakdown_cols[1]:
        st.caption("**📤 Emails sent by vertical**")
        if outreach_v.empty:
            st.caption(
                "_No sends logged. Run the 🔄 Gmail sync above to populate._"
            )
        else:
            st.line_chart(outreach_v, height=320)
else:
    st.info("No activity in the selected window.")

st.divider()

# ══════════════════════════════════════════════════════════════════════
# ROW 4 — Outreach location (where your sends are going)
# ══════════════════════════════════════════════════════════════════════

st.subheader("🌎 Where outreach is going")

loc = outreach_by_location(days=365)
if not (loc.get("by_state") or loc.get("by_city")):
    st.info(
        "No outreach logged yet. Run the 🔄 Gmail sync above to pull sends "
        "from your outbox — once logged, they're joined against the "
        "businesses table to surface geography."
    )
else:
    loc_cols = st.columns(2)
    with loc_cols[0]:
        st.caption("**By state (top 15)**")
        if loc.get("by_state"):
            df_st = pd.DataFrame(loc["by_state"])
            df_st["bounce %"] = df_st.apply(
                lambda r: f"{100*r['bounced']/r['sent']:.0f}%" if r["sent"] else "—",
                axis=1,
            )
            st.dataframe(
                df_st[["state", "sent", "bounced", "bounce %"]],
                use_container_width=True, hide_index=True,
                column_config={
                    "sent": st.column_config.ProgressColumn(
                        "Sent", min_value=0,
                        max_value=max(r["sent"] for r in loc["by_state"]),
                        format="%d",
                    ),
                },
            )
        else:
            st.caption("_No state data._")

    with loc_cols[1]:
        st.caption("**By city (top 15)**")
        if loc.get("by_city"):
            df_ct = pd.DataFrame(loc["by_city"])
            df_ct["bounce %"] = df_ct.apply(
                lambda r: f"{100*r['bounced']/r['sent']:.0f}%" if r["sent"] else "—",
                axis=1,
            )
            st.dataframe(
                df_ct[["city", "sent", "bounced", "bounce %"]],
                use_container_width=True, hide_index=True,
                column_config={
                    "sent": st.column_config.ProgressColumn(
                        "Sent", min_value=0,
                        max_value=max(r["sent"] for r in loc["by_city"]),
                        format="%d",
                    ),
                },
            )
        else:
            st.caption("_No city data._")

st.divider()

# ══════════════════════════════════════════════════════════════════════
# ROW 5 — Per-search table (enriched)
# ══════════════════════════════════════════════════════════════════════

st.subheader("Recent searches")

searches = enriched_searches(limit=50)
if not searches:
    st.info("No searches yet. Head to **🔎 Find Businesses** to start.")
else:
    table = []
    for s in searches:
        sent = s.get("sent", 0) or 0
        biz = s.get("biz_count", 0) or 0
        table.append({
            "#": s["id"],
            "Query": s["query"],
            "Location": s.get("location") or "",
            "Created": str(s.get("created_at") or "")[:10],
            "Businesses": biz,
            "Scraped": s.get("scraped", 0) or 0,
            "Emails": s.get("emails_found", 0) or 0,
            "DMs": s.get("dms_identified", 0) or 0,
            "Safe to send": s.get("safe_to_send", 0) or 0,
            "Sent": sent,
            "Bounced": s.get("bounced", 0) or 0,
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
# Workflow intro
# ══════════════════════════════════════════════════════════════════════

with st.expander("📖 Workflow + how this tool works"):
    st.markdown("""
### Workflow

1. **🔎 Find Businesses** — Google Maps search by type + location. Short queries like "law" fan out to 9 synonyms automatically.
2. **🚀 Bulk Scrape** — Run 200+ in background. **Volume mode** (recommended) for cheap mass outreach; **Triangulation** for high-stakes targets.
3. **📥 Export CSV** — 23-column CSV ready for your outreach platform or the audit tool.
4. **🔄 Sync from Gmail** (top of dashboard) — pulls real sends + bounces into the DB so these charts populate.
5. **📊 Pattern Learning** — Bounce analytics feed future runs.
6. **🔁 Replay** — Re-run past searches with new logic at ~$0 cost.

### Confidence tiers (Volume mode)

| Badge | Meaning | Safe to send? |
|---|---|---|
| 🟢 HIGH | NB returned VALID | Yes |
| 🟣 REVIEW | NB returned UNKNOWN | No — manual check first |
| 🟡 MEDIUM | Catchall domain / untested | Risky |
| 🔴 LOW | Industry-prior guess | Spray at your own risk |
| ⚫ EMPTY | Pipeline found nothing | Skip |

Generic inboxes (info@, contact@, hello@, smile@, alumni@, *any local with 'info' in it*) are **never** picked. Decision makers first; industry prior is the last resort.
    """)
