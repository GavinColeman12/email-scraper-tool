"""
Replay & regression tool.

Re-runs triangulation against a historical search and shows a before/after
diff. Near-zero cost because Phase 1-3 caches hold the original run's data.
Use this to A/B every logic change without waiting for the next live
campaign or spending API credits.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from src import storage
from src.replay_storage import list_replays, get_replay, init_replay_tables
from scripts.replay_search import run_replay


st.set_page_config(page_title="Replay", page_icon="🔁", layout="wide")
st.title("🔁 Replay & regression")
st.caption(
    "Re-run triangulation against a past search to A/B every logic change. "
    "Because Phase 1–3 caches live 14–90 days, replays cost ~$0. "
    "Capture a baseline before landing a change, then replay after to measure the lift."
)

storage.init_db()
init_replay_tables()

searches = storage.list_searches()
if not searches:
    st.warning("No searches yet. Go to **🔎 Find Businesses** first.")
    st.stop()

tab_new, tab_compare = st.tabs(["🆕 Run a replay", "📊 Compare replays"])

with tab_new:
    labels = {s["id"]: f"#{s['id']} — {s['query']} ({s.get('business_count', 0)} biz)"
              for s in searches}
    c1, c2, c3 = st.columns([3, 2, 1])
    search_id = c1.selectbox(
        "Search to replay",
        options=list(labels.keys()),
        format_func=lambda k: labels[k],
    )
    label = c2.text_input("Label", value="replay-manual",
                          help="Describe what this replay represents — "
                               "e.g. 'baseline' or 'post-L1-L8'")
    limit = c3.number_input("Limit", min_value=0, value=0, step=5,
                            help="0 = replay all businesses in this search")

    if st.button("🔁 Run replay", type="primary"):
        placeholder = st.empty()
        placeholder.info("Running… progress streams to stderr; check the terminal "
                         "for per-business output. Full replay of 30 biz ≈ 1–2 min.")
        try:
            replay_id = run_replay(
                search_id=int(search_id),
                label=label or "replay",
                limit=int(limit) if limit else None,
                verbose=False,
            )
        except Exception as e:
            placeholder.error(f"Replay failed: {e}")
            st.stop()
        placeholder.success(f"✅ Replay #{replay_id} saved")
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
                "id": r["id"],
                "search": r["original_search_id"],
                "label": r.get("label") or "",
                "git": (r.get("git_sha") or "")[:7],
                "created": str(r["created_at"])[:19],
                "n": m.get("n"),
                "safe%": m.get("safe_to_send_pct"),
                "dm%": m.get("dm_email_pct"),
                "generic%": m.get("generic_email_pct"),
                "pattern%": m.get("pattern_detected_pct"),
            })
        st.dataframe(rows, use_container_width=True, hide_index=True)

with tab_compare:
    replays = list_replays()
    if len(replays) < 2:
        st.info("Run at least 2 replays to compare.")
        st.stop()

    rep_labels = {r["id"]: f"#{r['id']} ({r.get('label','?')}) · search {r['original_search_id']}"
                  for r in replays}
    c1, c2 = st.columns(2)
    a_id = c1.selectbox("Before (A)", options=list(rep_labels.keys()),
                        format_func=lambda k: rep_labels[k], index=min(1, len(replays)-1))
    b_id = c2.selectbox("After (B)",  options=list(rep_labels.keys()),
                        format_func=lambda k: rep_labels[k], index=0)

    if a_id == b_id:
        st.warning("Pick two different replays.")
        st.stop()

    a = get_replay(int(a_id))
    b = get_replay(int(b_id))
    ma = (a.get("metrics") or {}).get("replay", {})
    mb = (b.get("metrics") or {}).get("replay", {})

    st.subheader("Metric deltas")
    kcols = st.columns(4)
    key_metrics = [
        ("safe_to_send_pct", "Safe to send", "↑ is better"),
        ("dm_email_pct",     "DM email picked", "↑ is better"),
        ("generic_email_pct","Generic inbox picked", "↓ is better"),
        ("pattern_detected_pct", "Pattern triangulated", "↑ is better"),
    ]
    for i, (k, label, direction) in enumerate(key_metrics):
        va, vb = ma.get(k, 0), mb.get(k, 0)
        delta = round(vb - va, 1)
        kcols[i].metric(label, f"{vb}%", delta=f"{delta:+.1f}",
                        help=f"Before: {va}% · After: {vb}% · {direction}")

    st.divider()
    st.subheader("Per-business changes")
    am = {r["replay"]["business_id"]: r for r in (a.get("businesses") or [])}
    bm = {r["replay"]["business_id"]: r for r in (b.get("businesses") or [])}
    changes = []
    for biz_id, bb in bm.items():
        aa = am.get(biz_id)
        if not aa:
            continue
        ae = aa["replay"].get("best_email") or "—"
        be = bb["replay"].get("best_email") or "—"
        if ae != be:
            changes.append({
                "business": bb["replay"].get("business_name", "")[:40],
                "before": ae,
                "after": be,
                "before_safe": aa["replay"].get("safe_to_send"),
                "after_safe":  bb["replay"].get("safe_to_send"),
            })
    if changes:
        st.write(f"**{len(changes)} businesses got a different email pick.**")
        st.dataframe(changes, use_container_width=True, hide_index=True)
    else:
        st.info("No per-business email changes between these two replays.")
