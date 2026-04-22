"""
🔁 Replay — inspect, compare, and explain every decision the pipeline made.

Previously a raw data dump. Now focused on three operator questions:
  1. Why did this business produce (or fail to produce) an email?
  2. What changed between two runs, and why did each row flip?
  3. Which "exhausted / no email" cases are worth re-running?
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from src import storage
from src.replay_storage import list_replays, get_replay, init_replay_tables
from src.replay_explain import (
    explain_biz, explain_change, bucket_label, _tier_rank,
)
from scripts.replay_search import run_replay


st.set_page_config(page_title="Replay", page_icon="🔁", layout="wide")
st.title("🔁 Replay — inspect & compare runs")
st.caption(
    "Re-runs the triangulation pipeline against a past search and explains "
    "every decision. Near-zero cost (Phase 1–3 caches live 14–90 days). "
    "Use it to verify logic changes before running a live campaign."
)

storage.init_db()
init_replay_tables()

searches = storage.list_searches()
if not searches:
    st.warning("No searches yet. Go to **🔎 Find Businesses** first.")
    st.stop()

tab_new, tab_inspect, tab_compare = st.tabs([
    "🆕 Run a replay",
    "🔍 Inspect one replay",
    "⚖️ Compare two replays",
])

# ══════════════════════════════════════════════════════════════════════
# TAB 1 — Run a replay
# ══════════════════════════════════════════════════════════════════════
with tab_new:
    st.markdown(
        "**Replay pulls from caches — no SearchApi credits burned, minimal NB.** "
        "Pick a search, label the run, and hit Go. Large searches take ~30–60s/biz."
    )
    labels = {s["id"]: f"#{s['id']} — {s['query']} · {s.get('business_count', 0)} biz"
              for s in searches}
    c1, c2, c3 = st.columns([3, 2, 1])
    search_id = c1.selectbox(
        "Search to replay",
        options=list(labels.keys()),
        format_func=lambda k: labels[k],
    )
    label = c2.text_input(
        "Label", value="",
        placeholder="baseline / post-bug-a-fix / retry-exhausted",
        help="Short tag so you can find this replay later. "
             "Good conventions: 'baseline-<date>', 'post-<fix>', 'retry-<scope>'.",
    )
    limit = c3.number_input("Limit", min_value=0, value=0, step=5,
                            help="0 = replay every business in the search")

    if st.button("🔁 Run replay", type="primary",
                 disabled=not label.strip(),
                 help="Labeled replays are searchable later; enter a label to enable."):
        with st.spinner(
            "Running replay… each biz takes 30-60s. Progress streams to "
            "the terminal. A 60-biz replay is ~30-45 min."
        ):
            try:
                replay_id = run_replay(
                    search_id=int(search_id),
                    label=label.strip() or "replay",
                    limit=int(limit) if limit else None,
                    verbose=False,
                )
            except Exception as e:
                st.error(f"Replay failed: {e}")
                st.stop()
        st.success(f"✅ Replay #{replay_id} saved. Switch to **🔍 Inspect** to read it.")
        st.rerun()

    st.divider()
    st.subheader("Past replays")

    replays = list_replays()
    if not replays:
        st.caption("No replays yet.")
    else:
        rows = []
        for r in replays[:50]:
            m = (r.get("metrics") or {}).get("replay", {})
            rows.append({
                "#": r["id"],
                "Search": r["original_search_id"],
                "Label": r.get("label") or "",
                "Created": str(r["created_at"])[:16],
                "Git": (r.get("git_sha") or "")[:7],
                "Biz": m.get("n"),
                "DM %": m.get("dm_email_pct"),
                "Generic %": m.get("generic_email_pct"),
                "Safe %": m.get("safe_to_send_pct"),
                "Pattern %": m.get("pattern_detected_pct"),
            })
        st.dataframe(rows, use_container_width=True, hide_index=True,
                     column_config={
                         "DM %": st.column_config.NumberColumn(format="%.1f%%"),
                         "Generic %": st.column_config.NumberColumn(format="%.1f%%"),
                         "Safe %": st.column_config.NumberColumn(format="%.1f%%"),
                         "Pattern %": st.column_config.NumberColumn(format="%.1f%%"),
                     })


# ══════════════════════════════════════════════════════════════════════
# TAB 2 — Inspect a single replay
# ══════════════════════════════════════════════════════════════════════
with tab_inspect:
    replays = list_replays()
    if not replays:
        st.info("No replays yet. Run one in the **🆕 Run a replay** tab.")
        st.stop()

    rep_labels = {r["id"]: f"#{r['id']} · {r.get('label','?')} · search {r['original_search_id']}"
                  for r in replays}
    inspect_id = st.selectbox(
        "Pick a replay to inspect",
        options=list(rep_labels.keys()),
        format_func=lambda k: rep_labels[k],
        key="_inspect_pick",
    )

    full = get_replay(int(inspect_id))
    if not full:
        st.error("Replay not found."); st.stop()

    businesses = full.get("businesses") or []
    metrics = (full.get("metrics") or {}).get("replay", {})
    st.markdown(
        f"**Replay #{full['id']}** · label `{full.get('label')}` · git "
        f"`{(full.get('git_sha') or '')[:7]}` · {metrics.get('n','?')} businesses"
    )

    # Headline metrics
    mc = st.columns(5)
    mc[0].metric("Businesses", metrics.get("n", 0))
    mc[1].metric("Safe to send", f"{metrics.get('safe_to_send_pct', 0):.1f}%")
    mc[2].metric("DM email", f"{metrics.get('dm_email_pct', 0):.1f}%")
    mc[3].metric("Generic inbox", f"{metrics.get('generic_email_pct', 0):.1f}%",
                  delta_color="inverse")
    mc[4].metric("Pattern triangulated", f"{metrics.get('pattern_detected_pct', 0):.1f}%")

    # Explain every biz
    st.divider()
    st.subheader("Per-business decisions (with WHY)")

    # Build the explanation table
    explained = []
    for row in businesses:
        rep = row.get("replay") or {}
        exp = explain_biz(rep)
        explained.append({
            "Business": (rep.get("business_name") or "")[:40],
            "Status": exp.severity,
            "Email": rep.get("best_email") or "—",
            "DM": exp.dm_name or "—",
            "Winning bucket": bucket_label(exp.winning_bucket) if exp.winning_bucket else "—",
            "NB": exp.nb_result or "—",
            "Reason": exp.reason,
            "Candidates": exp.candidate_summary,
            "_status": exp.status,
        })

    # Filters
    fc1, fc2, fc3 = st.columns(3)
    status_filter = fc1.multiselect(
        "Filter by status",
        ["found", "empty"],
        default=["found", "empty"],
    )
    severity_filter = fc2.multiselect(
        "Severity",
        ["ok", "warn", "fail"],
        default=["ok", "warn", "fail"],
        help="ok = NB-valid · warn = catchall/unknown/no DM · fail = all patterns bounced",
    )
    search_str = fc3.text_input("🔎 Filter by business name or email", "")

    filtered = [
        e for e in explained
        if e["_status"] in status_filter
        and e["Status"] in severity_filter
        and (not search_str
             or search_str.lower() in e["Business"].lower()
             or search_str.lower() in (e["Email"] or "").lower())
    ]

    # Status icon map for the Status column
    icon = {"ok": "🟢", "warn": "🟡", "fail": "🔴"}
    for e in filtered:
        e["Status"] = f"{icon.get(e['Status'], '⚫')} {e['Status']}"

    st.dataframe(
        [{k: v for k, v in e.items() if not k.startswith("_")} for e in filtered],
        use_container_width=True, hide_index=True,
        column_config={
            "Reason": st.column_config.TextColumn("Reason", width="large"),
            "Candidates": st.column_config.TextColumn("All candidates", width="medium"),
        },
    )
    st.caption(
        f"Showing {len(filtered)} / {len(explained)} businesses. "
        "The **Reason** column explains each decision; the **Candidates** "
        "column compact-lists every email the pipeline built and its NB verdict."
    )


# ══════════════════════════════════════════════════════════════════════
# TAB 3 — Compare two replays
# ══════════════════════════════════════════════════════════════════════
with tab_compare:
    replays = list_replays()
    if len(replays) < 2:
        st.info("Run at least 2 replays to compare.")
        st.stop()

    rep_labels = {r["id"]: f"#{r['id']} · {r.get('label','?')} · search {r['original_search_id']}"
                  for r in replays}

    cc1, cc2 = st.columns(2)
    a_id = cc1.selectbox("Before (A)", options=list(rep_labels.keys()),
                         format_func=lambda k: rep_labels[k],
                         index=min(1, len(replays)-1), key="_cmp_a")
    b_id = cc2.selectbox("After (B)",  options=list(rep_labels.keys()),
                         format_func=lambda k: rep_labels[k],
                         index=0, key="_cmp_b")
    if a_id == b_id:
        st.warning("Pick two different replays."); st.stop()

    a = get_replay(int(a_id))
    b = get_replay(int(b_id))
    ma = (a.get("metrics") or {}).get("replay", {})
    mb = (b.get("metrics") or {}).get("replay", {})

    # Headline deltas
    st.subheader("Headline deltas (after vs before)")
    key_metrics = [
        ("safe_to_send_pct",     "Safe to send",     "↑"),
        ("dm_email_pct",         "DM email",         "↑"),
        ("generic_email_pct",    "Generic inbox",    "↓"),
        ("pattern_detected_pct", "Pattern detected", "↑"),
    ]
    mcols = st.columns(len(key_metrics))
    for i, (k, label, want) in enumerate(key_metrics):
        va, vb = ma.get(k, 0) or 0, mb.get(k, 0) or 0
        delta = round(vb - va, 1)
        # For 'generic', ↓ is good → reverse delta color
        color = "inverse" if want == "↓" else "normal"
        mcols[i].metric(label, f"{vb}%", delta=f"{delta:+.1f}%",
                        delta_color=color)

    st.divider()

    # Per-biz explanations
    am_biz = {r["replay"].get("business_id"): r
              for r in (a.get("businesses") or [])}
    bm_biz = {r["replay"].get("business_id"): r
              for r in (b.get("businesses") or [])}

    # Four categories of change
    gains = []       # newly_found (empty → something)
    losses = []      # newly_lost  (something → empty)
    flips = []       # email_changed
    tier_ups = []    # same email, tier improved
    tier_downs = []  # same email, tier regressed

    for biz_id, bb in bm_biz.items():
        aa = am_biz.get(biz_id)
        if not aa:
            continue
        before_rep = aa.get("replay") or {}
        after_rep = bb.get("replay") or {}
        chg = explain_change(before_rep, after_rep)
        row = {
            "Business": (after_rep.get("business_name") or "")[:40],
            "Before": before_rep.get("best_email") or "—",
            "After": after_rep.get("best_email") or "—",
            "Tier before": (before_rep.get("confidence_tier") or "").replace("volume_", ""),
            "Tier after": (after_rep.get("confidence_tier") or "").replace("volume_", ""),
            "Why": chg.reason,
        }
        if chg.change_type == "newly_found":
            gains.append(row)
        elif chg.change_type == "newly_lost":
            losses.append(row)
        elif chg.change_type == "email_changed":
            flips.append(row)
        elif chg.change_type == "tier_changed":
            if chg.severity == "gain":
                tier_ups.append(row)
            else:
                tier_downs.append(row)

    def _render(title: str, rows: list, emoji: str, caption: str):
        st.markdown(f"### {emoji} {title} ({len(rows)})")
        st.caption(caption)
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True,
                         column_config={
                             "Why": st.column_config.TextColumn("Why", width="large"),
                         })
        else:
            st.caption("_(none)_")

    _render("Gains — newly found emails", gains, "🟢",
            "Rows that were empty in A but produced an email in B.")
    _render("Losses — newly empty", losses, "🔴",
            "Rows that had an email in A but are now empty. Watch for regressions.")
    _render("Pick flipped", flips, "🔀",
            "Different email picked. Reason column explains the ranking shift.")
    _render("Tier upgraded (same email)", tier_ups, "📈",
            "NB verdict improved — same pick, better confidence.")
    _render("Tier downgraded (same email)", tier_downs, "📉",
            "NB verdict worsened — e.g. catchall → unknown, or valid → unknown.")

    st.divider()
    st.caption(
        f"**Summary:** {len(gains)} gains, {len(losses)} losses, "
        f"{len(flips)} flips, {len(tier_ups)} tier-ups, {len(tier_downs)} tier-downs. "
        f"**Net email change:** {len(gains) - len(losses):+d}."
    )
