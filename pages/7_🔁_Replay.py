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

import io

import pandas as pd
import streamlit as st

from src import storage
from src.replay_storage import list_replays, get_replay, init_replay_tables
from src.replay_explain import (
    explain_biz, explain_change, bucket_label,
)
from src.dashboard_queries import search_metadata
from scripts.replay_search import run_replay, REPLAY_MODES


# ──────────────────────────────────────────────────────────────────────
# Helpers — classify rows + build export-ready CSV dicts
# ──────────────────────────────────────────────────────────────────────

def _is_nb_valid(rep: dict) -> bool:
    """A row counts as 'reachable' if NB confirmed valid on the picked
    email. Catchall / unknown / invalid all treated as not-yet-reachable."""
    return ((rep.get("best_email_nb_result") or "").lower() == "valid"
            and bool(rep.get("best_email")))


def _outcome(before: dict, after: dict) -> str:
    """Bucket every row into the 4 operator-meaningful outcomes:
      newly_caught — empty / non-valid in A → NB-valid in B  (the WIN)
      stable_good  — NB-valid in both runs (same OR different email)
      regressed    — NB-valid in A → not-valid in B
      still_empty  — neither run produced an NB-valid email
    """
    a_good = _is_nb_valid(before)
    b_good = _is_nb_valid(after)
    if not a_good and b_good:
        return "newly_caught"
    if a_good and b_good:
        return "stable_good"
    if a_good and not b_good:
        return "regressed"
    return "still_empty"


def _export_row(biz_id, before: dict, after: dict, biz_lookup: dict) -> dict:
    """Flatten a before+after pair into a CSV-friendly dict the user
    can drop into their outreach tool. Pulls original biz address /
    phone / website from storage so the export is self-contained."""
    biz = biz_lookup.get(biz_id) or {}
    dm = (after.get("decision_maker") or {}) if after.get("decision_maker") else {}
    return {
        "Business": after.get("business_name") or biz.get("business_name") or "",
        "Address": biz.get("address") or "",
        "Phone": biz.get("phone") or "",
        "Website": biz.get("website") or after.get("website") or "",
        "Email (new run)": after.get("best_email") or "",
        "Email (old run)": before.get("best_email") or "",
        "NB result": (after.get("best_email_nb_result") or "").lower() or "—",
        "DM name": dm.get("full_name") or "",
        "DM title": dm.get("title") or "",
        "Tier (old)": (before.get("confidence_tier") or "").replace("volume_", ""),
        "Tier (new)": (after.get("confidence_tier") or "").replace("volume_", ""),
        "Business ID": biz_id,
    }


def _csv_bytes(rows: list[dict]) -> bytes:
    if not rows:
        return b""
    buf = io.StringIO()
    pd.DataFrame(rows).to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


# Short human descriptions for each mode (used in the selector help text).
MODE_HELP = {
    "volume":        "🚀 Volume (RECOMMENDED) — crawl + Wayback + NPI + NB (~30s/biz, <$4/200).",
    "basic":         "⚡ Basic — rules + SMTP only (~5s/biz, free).",
    "verified":      "✅ Verified — rules + Haiku + SMTP (~6s/biz, ~$0.30/200).",
    "deep":          "🧠 Deep — 4 agents + Sonnet + SMTP (~10s/biz, ~$2/200).",
    "triangulation": "⚠️ Triangulation (DEPRECATED) — 5 agents, ~$5-6/100 (5× volume's cost).",
}


st.set_page_config(page_title="Replay", page_icon="🔁", layout="wide")

from src.job_status_widget import render_jobs_sidebar
render_jobs_sidebar()
st.title("🔁 Replay — inspect & compare runs")

with st.expander("ℹ️ How Replay works (read me first)", expanded=False):
    st.markdown(
        """
**Replay re-runs a past search through today's pipeline** so you can see
how scraper changes affect results — without spending more SearchApi
credits (Phase 1-3 caches live 14-90 days).

**Three tabs, three jobs:**

- **🆕 Run a replay** — kick off a new replay against any past search.
  Pick the search, pick the mode (volume / triangulation / etc.), give
  it a label (e.g. `post-NPI-fix`), hit Go. Past replays appear in a
  table at the bottom — click any row to jump to inspect or compare.

- **🔍 Inspect one replay** — see every business in a single replay
  with the WHY column explaining each decision. Export send-safe rows,
  empty rows, or the full set as CSV. Your selection persists if you
  navigate to other pages and come back.

- **⚖️ Compare two replays** — diff two replays of the same search to
  see what your pipeline changes did: 🟢 newly caught, ⚪ stable, 🔴
  regressed, ⚫ still empty. Download the wins as CSV (the rows the
  new run caught that the old missed).

**Cost:** ~$0 per replay. Safe to run as many as you want.
"""
    )

st.caption(
    "Re-runs the pipeline against a past search and explains every "
    "decision. Use it to verify logic changes before running a live campaign."
)

storage.init_db()
init_replay_tables()

searches = storage.list_searches()
if not searches:
    st.warning("No searches yet. Go to **🔎 Find Businesses** first.")
    st.stop()

# Read deep-link params so the past-replays table can jump users to a
# specific tab + replay (st.query_params persists across page navigation
# inside the same Streamlit session).
qp = st.query_params
default_tab_idx = 0
if qp.get("tab") == "inspect":
    default_tab_idx = 1
elif qp.get("tab") == "compare":
    default_tab_idx = 2

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
        "Pick a search, choose a mode, label the run. Use this to A/B an old "
        "campaign against today's logic — e.g. re-run an old triangulation "
        "search with the new volume scraper."
    )

    # Enriched dropdown: biz count + primary industry + date + query.
    meta = search_metadata([s["id"] for s in searches])

    def _fmt_search(sid: int) -> str:
        m = meta.get(int(sid)) or {}
        created = str(m.get("created_at") or "")[:10]
        biz = m.get("biz_count") or 0
        ind = m.get("primary_industry") or "—"
        q = (m.get("query") or "").strip()[:40]
        return f"#{sid} · {created} · {biz} biz · {ind} · {q}"

    c1, c2, c3 = st.columns([3, 2, 1])
    search_id = c1.selectbox(
        "Search to replay",
        options=[s["id"] for s in searches],
        format_func=_fmt_search,
    )
    label = c2.text_input(
        "Label (optional)", value="",
        placeholder="leave blank for auto: replay-<mode>-<date>",
        help="Optional tag. Leave blank and we auto-generate "
             "'replay-<mode>-<YYYYMMDD-HHMM>' so the row is still searchable.",
    )
    limit = c3.number_input("Limit", min_value=0, value=0, step=5,
                            help="0 = replay every business in the search")

    # Mode selector — defaults to volume (cheap, same columns as triangulation,
    # best for "what would today's volume scraper do with this old campaign").
    mode = st.radio(
        "Which pipeline to re-run with",
        options=list(REPLAY_MODES),
        index=list(REPLAY_MODES).index("volume"),
        format_func=lambda m: MODE_HELP.get(m, m),
        horizontal=False,
        help="Pick any mode — Compare will let you diff two modes on the "
             "same search (e.g. old triangulation vs new volume).",
    )

    if st.button("🔁 Run replay", type="primary"):
        from datetime import datetime as _dt
        run_label = (
            label.strip()
            or f"replay-{mode}-{_dt.now().strftime('%Y%m%d-%H%M')}"
        )
        with st.spinner(
            f"Running {mode} replay as '{run_label}'… each biz takes ~30-60s. "
            "Progress streams to the terminal. A 60-biz replay is ~30-45 min."
        ):
            try:
                replay_id = run_replay(
                    search_id=int(search_id),
                    label=run_label,
                    limit=int(limit) if limit else None,
                    verbose=False,
                    mode=mode,
                )
            except Exception as e:
                st.error(f"Replay failed: {e}")
                st.stop()
        st.success(f"✅ Replay #{replay_id} ({mode}) saved. Switch to **🔍 Inspect** to read it.")
        st.rerun()

    st.divider()
    st.subheader("Past replays")

    replays = list_replays()
    if not replays:
        st.caption("No replays yet.")
    else:
        past_meta = search_metadata([r["original_search_id"] for r in replays[:50]])
        rows = []
        for r in replays[:50]:
            m = (r.get("metrics") or {}).get("replay", {})
            sm = past_meta.get(int(r["original_search_id"])) or {}
            rows.append({
                "#": r["id"],
                "Search": r["original_search_id"],
                "Industry": sm.get("primary_industry") or "—",
                "Mode": r.get("mode") or "triangulation",
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

        # Navigation hub — pick a replay and jump to Inspect or Compare
        st.markdown("##### Open a past replay")
        nav_labels = {r["id"]: f"#{r['id']} · {r.get('mode') or 'triangulation'} "
                                f"· {r.get('label') or 'unlabeled'} "
                                f"(search {r['original_search_id']})"
                       for r in replays[:50]}
        nc1, nc2 = st.columns([3, 2])
        with nc1:
            jump_id = st.selectbox(
                "Replay",
                options=list(nav_labels.keys()),
                format_func=lambda k: nav_labels[k],
                key="_run_tab_nav_pick",
            )
        with nc2:
            jc1, jc2 = st.columns(2)
            if jc1.button("🔍 Inspect", use_container_width=True):
                st.query_params["tab"] = "inspect"
                st.query_params["inspect_id"] = str(jump_id)
                st.rerun()
            # Default Compare partner: most recent prior replay of same search
            same = [r for r in replays
                    if r["original_search_id"] == next(
                        (rr["original_search_id"] for rr in replays
                         if rr["id"] == jump_id), None
                    )
                    and r["id"] != jump_id]
            if jc2.button(
                "⚖️ Compare", use_container_width=True,
                disabled=not same,
                help="Diff against the most recent prior replay of the "
                     "same search. Disabled when no prior replay exists.",
            ):
                # Pick the most recent partner
                partner = sorted(
                    same, key=lambda r: r.get("created_at") or "",
                    reverse=True,
                )[0]
                st.query_params["tab"] = "compare"
                st.query_params["cmp_a"] = str(partner["id"])
                st.query_params["cmp_b"] = str(jump_id)
                st.rerun()


# ══════════════════════════════════════════════════════════════════════
# TAB 2 — Inspect a single replay
# ══════════════════════════════════════════════════════════════════════
with tab_inspect:
    replays = list_replays()
    if not replays:
        st.info("No replays yet. Run one in the **🆕 Run a replay** tab.")
        st.stop()

    meta_i = search_metadata([r["original_search_id"] for r in replays])
    def _fmt_rep(r: dict) -> str:
        sm = meta_i.get(int(r["original_search_id"])) or {}
        mode_tag = r.get("mode") or "triangulation"
        return (f"#{r['id']} · {mode_tag} · {sm.get('primary_industry') or '—'} "
                f"· {sm.get('biz_count') or 0} biz · {r.get('label') or '?'} "
                f"(search {r['original_search_id']})")
    rep_labels = {r["id"]: _fmt_rep(r) for r in replays}

    # Persist selection across navigation via query_params + session_state.
    # Priority: ?inspect_id=N URL param > prior session state > newest replay.
    qp_inspect = qp.get("inspect_id")
    if qp_inspect:
        try:
            qp_id = int(qp_inspect)
            if qp_id in rep_labels:
                st.session_state["_inspect_pick"] = qp_id
        except Exception:
            pass

    inspect_id = st.selectbox(
        "Pick a replay to inspect",
        options=list(rep_labels.keys()),
        format_func=lambda k: rep_labels[k],
        key="_inspect_pick",
        help="Selection persists if you navigate to another page and come back.",
    )
    # Mirror selection into URL so refreshes / back-button preserve it
    st.query_params["inspect_id"] = str(inspect_id)

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

    # ── Inspect-tab downloads ──────────────────────────────────────────
    # Build a richer export shape (with biz address / phone / website
    # from storage) so the CSV is self-contained for outreach tooling.
    st.divider()
    st.subheader("📥 Download from this replay")
    st.caption(
        "Export rows from the current replay (no diff). Use Compare "
        "for the 'newly caught vs old run' diff exports."
    )

    inspect_search_id = full.get("original_search_id")
    biz_lookup_inspect: dict = {}
    if inspect_search_id:
        for biz in storage.list_businesses(search_id=int(inspect_search_id)):
            biz_lookup_inspect[biz["id"]] = biz

    def _inspect_export_row(row_replay: dict) -> dict:
        """Single-replay export row — same schema as Compare for
        downstream consistency, but Before/After collapse into one
        Email column since there's no diff here."""
        biz_id = row_replay.get("business_id")
        biz = biz_lookup_inspect.get(biz_id) or {}
        dm = (row_replay.get("decision_maker") or {}) if row_replay.get("decision_maker") else {}
        return {
            "Business": row_replay.get("business_name") or biz.get("business_name") or "",
            "Address": biz.get("address") or "",
            "Phone": biz.get("phone") or "",
            "Website": biz.get("website") or row_replay.get("website") or "",
            "Email": row_replay.get("best_email") or "",
            "NB result": (row_replay.get("best_email_nb_result") or "").lower() or "—",
            "DM name": dm.get("full_name") or "",
            "DM title": dm.get("title") or "",
            "Tier": (row_replay.get("confidence_tier") or "").replace("volume_", ""),
            "Business ID": biz_id,
        }

    # Bucket every row by send-readiness
    nb_valid_rows = []        # Send-safe today
    found_rows = []           # Has any email (NB-valid + others)
    empty_rows = []           # Zero candidates produced
    all_rows = []
    for row in businesses:
        rep = row.get("replay") or {}
        export = _inspect_export_row(rep)
        all_rows.append(export)
        if rep.get("best_email"):
            found_rows.append(export)
            if (rep.get("best_email_nb_result") or "").lower() == "valid":
                nb_valid_rows.append(export)
        else:
            empty_rows.append(export)

    rid = full["id"]
    dl1, dl2, dl3, dl4 = st.columns(4)
    with dl1:
        st.download_button(
            f"🟢 Send-safe NB-valid ({len(nb_valid_rows)})",
            data=_csv_bytes(nb_valid_rows),
            file_name=f"replay_{rid}_send_safe.csv",
            mime="text/csv",
            disabled=not nb_valid_rows,
            type="primary",
            help="Rows where NB confirmed the email is valid. "
                 "Drop straight into outreach.",
        )
    with dl2:
        st.download_button(
            f"📧 All with email ({len(found_rows)})",
            data=_csv_bytes(found_rows),
            file_name=f"replay_{rid}_with_email.csv",
            mime="text/csv",
            disabled=not found_rows,
            help="Every row that produced an email — includes "
                 "catchall, unknown, and untested. Review NB column.",
        )
    with dl3:
        st.download_button(
            f"📦 All rows ({len(all_rows)})",
            data=_csv_bytes(all_rows),
            file_name=f"replay_{rid}_all.csv",
            mime="text/csv",
            disabled=not all_rows,
            help="Full replay — every business including empties. "
                 "Useful for review + manual research.",
        )
    with dl4:
        st.download_button(
            f"⚫ Empty rows ({len(empty_rows)})",
            data=_csv_bytes(empty_rows),
            file_name=f"replay_{rid}_empty.csv",
            mime="text/csv",
            disabled=not empty_rows,
            help="Rows where the pipeline found no email. "
                 "Candidates for manual research / phone outreach.",
        )

    # ── Quick-compare shortcut ─────────────────────────────────────────
    st.divider()
    st.subheader("⚖️ Compare this replay to another")
    st.caption(
        "Diff this replay against another to see which rows the new "
        "run caught vs the old. Auto-suggests the most recent prior "
        "replay of the same search."
    )

    same_search_replays = [
        r for r in replays
        if r["original_search_id"] == full["original_search_id"]
        and r["id"] != full["id"]
    ]
    if same_search_replays:
        # Sort newest first; default to the most recent one
        prior = sorted(same_search_replays,
                        key=lambda r: r.get("created_at") or "",
                        reverse=True)
        prior_labels = {r["id"]: _fmt_rep(r) for r in prior}
        c1, c2 = st.columns([3, 1])
        with c1:
            other_id = st.selectbox(
                "Compare against (other replay of same search)",
                options=list(prior_labels.keys()),
                format_func=lambda k: prior_labels[k],
                key="_inspect_compare_pick",
            )
        with c2:
            if st.button("🔁 Open in Compare tab", type="primary"):
                # Set query params for both A and B and reload
                st.query_params["tab"] = "compare"
                st.query_params["cmp_a"] = str(other_id)
                st.query_params["cmp_b"] = str(rid)
                st.rerun()
    else:
        st.caption(
            "No other replays exist for this search yet. Run a second "
            "replay (different mode or after a code change) to compare."
        )


# ══════════════════════════════════════════════════════════════════════
# TAB 3 — Compare two replays
# ══════════════════════════════════════════════════════════════════════
with tab_compare:
    replays = list_replays()
    if len(replays) < 2:
        st.info("Run at least 2 replays to compare.")
        st.stop()

    meta_c = search_metadata([r["original_search_id"] for r in replays])
    def _fmt_rep_c(r: dict) -> str:
        sm = meta_c.get(int(r["original_search_id"])) or {}
        mode_tag = r.get("mode") or "triangulation"
        return (f"#{r['id']} · {mode_tag} · {sm.get('primary_industry') or '—'} "
                f"· {r.get('label') or '?'} (search {r['original_search_id']})")
    rep_labels = {r["id"]: _fmt_rep_c(r) for r in replays}

    # Honor deep-link from Inspect tab's "Open in Compare" button —
    # cmp_a / cmp_b query params override the default selection.
    qp_a = qp.get("cmp_a")
    qp_b = qp.get("cmp_b")
    if qp_a:
        try:
            qa = int(qp_a)
            if qa in rep_labels:
                st.session_state["_cmp_a"] = qa
        except Exception:
            pass
    if qp_b:
        try:
            qb = int(qp_b)
            if qb in rep_labels:
                st.session_state["_cmp_b"] = qb
        except Exception:
            pass

    cc1, cc2 = st.columns(2)
    a_id = cc1.selectbox("Before (A)", options=list(rep_labels.keys()),
                         format_func=lambda k: rep_labels[k],
                         index=min(1, len(replays)-1), key="_cmp_a",
                         help="Selection persists across page navigation.")
    b_id = cc2.selectbox("After (B)",  options=list(rep_labels.keys()),
                         format_func=lambda k: rep_labels[k],
                         index=0, key="_cmp_b")
    # Mirror back into URL for stable navigation
    st.query_params["cmp_a"] = str(a_id)
    st.query_params["cmp_b"] = str(b_id)
    if a_id == b_id:
        st.warning("Pick two different replays."); st.stop()

    a = get_replay(int(a_id))
    b = get_replay(int(b_id))
    ma = (a.get("metrics") or {}).get("replay", {})
    mb = (b.get("metrics") or {}).get("replay", {})

    # Mode + search provenance — tells the operator whether this is an
    # apples-to-apples comparison (same search, same mode) or a
    # cross-mode experiment (e.g. old triangulation vs new volume).
    a_mode = a.get("mode") or "triangulation"
    b_mode = b.get("mode") or "triangulation"
    a_search = a.get("original_search_id")
    b_search = b.get("original_search_id")
    if a_search == b_search and a_mode != b_mode:
        st.info(f"⚖️ Cross-mode replay of the same search #{a_search}: "
                f"**{a_mode}** → **{b_mode}**. Deltas show which pipeline "
                f"wins on this campaign with today's logic.")
    elif a_search != b_search:
        st.warning(f"Different source searches (A=#{a_search}, B=#{b_search}). "
                   "Deltas aren't apples-to-apples at the biz level — "
                   "only the aggregate percentages are meaningful.")

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

    # Pull the original biz rows once so exports include
    # address / phone / website without N+1 lookups.
    biz_lookup: dict = {}
    if a_search:
        for biz in storage.list_businesses(search_id=int(a_search)):
            biz_lookup[biz["id"]] = biz

    # Bucket every row by NB-valid outcome (the operator-meaningful split)
    newly_caught: list = []   # was non-valid in A, NB-valid in B  (THE WIN)
    stable_good: list = []    # NB-valid in both runs
    regressed: list = []      # NB-valid in A, lost it in B
    still_empty: list = []    # neither run produced NB-valid

    # Also keep the old fine-grained categories for the per-row tables —
    # they expose the "why" of each change beyond the simple outcome
    gains_finegrained = []     # email changed empty → anything
    flips = []                 # email changed something → something
    tier_ups = []
    tier_downs = []
    losses_finegrained = []

    for biz_id, bb in bm_biz.items():
        aa = am_biz.get(biz_id)
        if not aa:
            continue
        before_rep = aa.get("replay") or {}
        after_rep = bb.get("replay") or {}
        outcome = _outcome(before_rep, after_rep)
        export_row = _export_row(biz_id, before_rep, after_rep, biz_lookup)

        if outcome == "newly_caught":
            newly_caught.append(export_row)
        elif outcome == "stable_good":
            stable_good.append(export_row)
        elif outcome == "regressed":
            regressed.append(export_row)
        else:
            still_empty.append(export_row)

        # Fine-grained category for the per-row tables
        chg = explain_change(before_rep, after_rep)
        why_row = {
            **export_row,
            "Why": chg.reason,
        }
        if chg.change_type == "newly_found":
            gains_finegrained.append(why_row)
        elif chg.change_type == "newly_lost":
            losses_finegrained.append(why_row)
        elif chg.change_type == "email_changed":
            flips.append(why_row)
        elif chg.change_type == "tier_changed":
            (tier_ups if chg.severity == "gain" else tier_downs).append(why_row)

    # ── TOP-LEVEL OUTCOME PANEL — clear right-vs-wrong picture ──
    st.subheader("Right vs. wrong — outcome breakdown")
    o1, o2, o3, o4 = st.columns(4)
    o1.metric(
        "🟢 Newly caught",
        len(newly_caught),
        help="Rows where the OLD run failed (empty / non-NB-valid) but "
             "the NEW run produced a confirmed-deliverable email. These "
             "are the wins from your latest pipeline improvements.",
    )
    o2.metric(
        "⚪ Stable correct",
        len(stable_good),
        help="Rows where BOTH runs produced an NB-valid email — the "
             "pipeline got it right both times. Same email or different, "
             "either way deliverable.",
    )
    o3.metric(
        "🔴 Regressed",
        len(regressed),
        delta_color="inverse",
        help="Rows where the OLD run had a confirmed-deliverable email "
             "but the NEW run lost it. Watch this carefully — it means "
             "a recent change broke something that used to work.",
    )
    o4.metric(
        "⚫ Still unreachable",
        len(still_empty),
        help="Neither run could find a deliverable email. These are "
             "genuinely hard targets — defunct businesses, locked-down "
             "domains, or DMs with no online footprint.",
    )

    total = len(newly_caught) + len(stable_good) + len(regressed) + len(still_empty)
    if total:
        improved_pct = round(100 * len(newly_caught) / total, 1)
        st.caption(
            f"**{improved_pct}% improvement rate** — the new run picked "
            f"up {len(newly_caught)} of {total} rows the old run missed. "
            f"Net change: **{len(newly_caught) - len(regressed):+d}** "
            f"deliverable emails."
        )

    # ── DOWNLOAD WINS — the user's main ask ──
    st.markdown("### 📥 Download the wins")
    st.caption(
        "Export the rows the new run caught that the old one didn't. "
        "Drop straight into your outreach tool — includes business "
        "address, phone, website, DM name, and NB-confirmed email."
    )
    dl1, dl2, dl3 = st.columns(3)
    with dl1:
        st.download_button(
            f"🟢 Newly caught ({len(newly_caught)})",
            data=_csv_bytes(newly_caught),
            file_name=f"replay_newly_caught_A{a_id}_vs_B{b_id}.csv",
            mime="text/csv",
            disabled=not newly_caught,
            type="primary",
            help="The big win: rows the new pipeline caught that the "
                 "old one missed. NB-valid only, ready to send.",
        )
    with dl2:
        st.download_button(
            f"⚪ Stable wins ({len(stable_good)})",
            data=_csv_bytes(stable_good),
            file_name=f"replay_stable_good_A{a_id}_vs_B{b_id}.csv",
            mime="text/csv",
            disabled=not stable_good,
            help="Rows both runs got right. Useful to merge into your "
                 "send list alongside the newly-caught wins.",
        )
    with dl3:
        st.download_button(
            f"🔴 Regressed ({len(regressed)})",
            data=_csv_bytes(regressed),
            file_name=f"replay_regressed_A{a_id}_vs_B{b_id}.csv",
            mime="text/csv",
            disabled=not regressed,
            help="Rows the new run broke. Investigate before promoting "
                 "the new pipeline to production.",
        )

    # Combined "all wins" CSV — newly caught + stable
    if newly_caught or stable_good:
        all_wins = newly_caught + stable_good
        st.download_button(
            f"📦 All deliverable rows ({len(all_wins)}) — combined send list",
            data=_csv_bytes(all_wins),
            file_name=f"replay_all_deliverable_A{a_id}_vs_B{b_id}.csv",
            mime="text/csv",
            help="Newly caught + stable wins, deduplicated. Your full "
                 "send-ready list from this comparison.",
        )

    st.divider()

    # ── PER-CATEGORY TABLES with WHY column + per-table downloads ──
    st.subheader("Per-row breakdown with reasons")
    st.caption(
        "Same data, sliced finer so you can see WHY each row changed. "
        "Each table downloads as its own CSV."
    )

    def _render(title: str, rows: list, emoji: str, caption: str,
                file_slug: str):
        st.markdown(f"### {emoji} {title} ({len(rows)})")
        st.caption(caption)
        if rows:
            # Show a compact subset in the table — full data is in the CSV
            display_cols = [
                "Business", "Email (old run)", "Email (new run)",
                "NB result", "Tier (old)", "Tier (new)", "Why",
            ]
            df = pd.DataFrame(rows)
            display_df = df[[c for c in display_cols if c in df.columns]]
            st.dataframe(
                display_df, use_container_width=True, hide_index=True,
                column_config={
                    "Why": st.column_config.TextColumn("Why", width="large"),
                },
            )
            st.download_button(
                f"📥 Download {len(rows)} {title.lower()} as CSV",
                data=_csv_bytes(rows),
                file_name=f"replay_{file_slug}_A{a_id}_vs_B{b_id}.csv",
                mime="text/csv",
                key=f"dl_{file_slug}_{a_id}_{b_id}",
            )
        else:
            st.caption("_(none)_")

    _render("Newly found emails", gains_finegrained, "🟢",
            "Rows that were empty in A but produced an email in B "
            "(NB verdict may still be catchall/unknown — check NB column).",
            "gains")
    _render("Tier upgraded (same email)", tier_ups, "📈",
            "Same email picked, NB verdict improved (e.g. unknown → valid).",
            "tier_ups")
    _render("Pick flipped", flips, "🔀",
            "Different email picked. Why column explains the ranking shift.",
            "flips")
    _render("Tier downgraded (same email)", tier_downs, "📉",
            "Same email, NB verdict worsened. Often transient — re-run "
            "may recover.",
            "tier_downs")
    _render("Newly empty", losses_finegrained, "🔴",
            "Rows that had an email in A but are now empty. Investigate "
            "before promoting the new pipeline.",
            "losses")
