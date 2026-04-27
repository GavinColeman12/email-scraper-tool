"""
Shared job-status sidebar — renders on every page so the operator can
always see what's running, regardless of which tab they're viewing.

Usage (top of every page, AFTER st.set_page_config):

    from src.job_status_widget import render_jobs_sidebar
    render_jobs_sidebar()

The widget polls background_jobs.list_active() every 3s via
st.fragment(run_every=3) so progress bars tick live without a full
page rerun. When zero jobs are active, the sidebar shows a quiet
"no jobs running" line — no visual noise.

Supports multiple concurrent jobs — if the operator triggers a Bulk
Scrape AND a one-click Find Businesses pipeline at the same time,
both progress cards stack in the sidebar.
"""
from __future__ import annotations

import streamlit as st

from src import background_jobs


def _job_card(job: dict) -> None:
    """Render one job's status card. Compact + readable in the sidebar."""
    job_id = (job.get("id") or "")[:8]
    job_type = job.get("job_type") or "scrape"
    progress = int(job.get("progress") or 0)
    total = max(1, int(job.get("total") or 1))
    pct = int(100 * progress / total)
    success = int(job.get("success_count") or 0)
    errors = int(job.get("error_count") or 0)

    # Friendly job-type label
    type_label = {
        "bulk_volume_scrape": "🚀 Volume scrape",
        "bulk_triangulation_scrape": "🎯 Triangulation",
        "bulk_deep_scrape": "🧠 Deep scrape",
        "bulk_verified_scrape": "✅ Verified scrape",
        "bulk_scrape": "⚡ Basic scrape",
    }.get(job_type, job_type)

    # Search context — pulled from metadata so the sidebar shows what
    # the operator is actually scraping (e.g. "law in NYC")
    metadata = job.get("metadata_json") or job.get("metadata") or {}
    if isinstance(metadata, str):
        import json as _json
        try:
            metadata = _json.loads(metadata)
        except Exception:
            metadata = {}
    label = metadata.get("search_label") or f"job {job_id}"

    with st.container(border=True):
        st.markdown(f"**{type_label}**")
        st.caption(f"`{job_id}` · {label[:40]}")
        st.progress(min(pct, 100) / 100, text=f"{progress}/{total} ({pct}%)")
        m1, m2 = st.columns(2)
        m1.caption(f"✓ {success}")
        m2.caption(f"✗ {errors}" if errors else "")
        # Cancel button — quick way to stop a runaway job from
        # any page without navigating to Bulk Scrape
        if st.button(
            "🛑 Cancel", key=f"_cancel_{job_id}_{progress}",
            use_container_width=True,
        ):
            background_jobs.cancel(job["id"])
            st.toast(f"Cancelled job {job_id}", icon="🛑")


@st.fragment(run_every=3)
def _jobs_panel() -> None:
    """Live-updating fragment — re-runs every 3s while a job is active.
    When no jobs are running, this fragment renders nothing visible
    so it doesn't churn the page."""
    try:
        active = background_jobs.list_active() or []
    except Exception:
        active = []
    if not active:
        st.caption("_No jobs running._")
        return
    st.caption(
        f"**{len(active)}** running · auto-refreshing every 3s"
    )
    for job in active:
        _job_card(job)


def render_jobs_sidebar() -> None:
    """
    Drop into the sidebar of every page after st.set_page_config().
    Always visible, always live. Quiet when nothing's running.
    """
    with st.sidebar:
        st.markdown("### 🟢 Active jobs")
        _jobs_panel()
