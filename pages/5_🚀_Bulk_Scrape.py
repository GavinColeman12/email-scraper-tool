"""Page 5: Bulk scrape 200+ businesses in background, rank by quality, pick top N."""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from src import storage, background_jobs
from src.email_scraper import scrape_business_emails
from src.deep_scraper import deep_scrape_business_emails
# Lazy-imported inside the worker to avoid hard-blocking the page if the
# triangulation module has any import-time issue on a given runtime.
from src.lead_scoring import compute_lead_quality_score, rank_businesses
from src.secrets import get_secret
from src.export_rows import (
    build_rows, BULK_DISPLAY_COLUMNS, EXPORT_COLUMNS,
)

st.set_page_config(page_title="Bulk Scrape", page_icon="🚀", layout="wide")
st.title("🚀 Bulk Scrape + Pick Top N")
st.caption(
    "Run 200+ businesses in the background, score each lead 0-100 on quality, "
    "then filter to the highest-confidence leads. You can navigate away while it runs."
)

storage.init_db()
background_jobs.init_db()
try:
    background_jobs.cleanup_stale(max_age_hours=24)
except Exception:
    pass

# ── Active jobs banner ──
background_jobs.render_active_banner(st)

# ── Pick search ──
searches = storage.list_searches()
if not searches:
    st.warning("No searches yet. Go to **🔎 Find Businesses** first to create one.")
    st.stop()

labels = {s["id"]: f"#{s['id']} — {s['query']} ({s.get('with_email', 0)}/{s.get('business_count', 0)})"
          for s in searches}
search_id = st.selectbox("Search", options=list(labels.keys()),
                          format_func=lambda k: labels[k])

businesses = storage.list_businesses(search_id=search_id)
pending = [b for b in businesses if not b.get("primary_email") and b.get("website")]
scraped = [b for b in businesses if b.get("primary_email")]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total", len(businesses))
c2.metric("Pending", len(pending))
c3.metric("Scraped", len(scraped))
c4.metric("No website", len([b for b in businesses if not b.get("website")]))

# ── Check active job for this search ──
active = [
    j for j in background_jobs.list_active()
    if j.get("job_type") in ("bulk_scrape", "bulk_deep_scrape", "bulk_verified_scrape")
    and j.get("search_id") == search_id
]
running_job = active[0] if active else None

# ── Mode + concurrency ──
st.subheader("Scrape settings")

try:
    has_claude = bool(get_secret("ANTHROPIC_API_KEY"))
except Exception:
    has_claude = False

mode_col1, mode_col2 = st.columns([3, 2])
with mode_col1:
    mode = st.radio(
        "Mode",
        ["basic", "verified", "deep", "triangulation"],
        format_func=lambda k: {
            "basic": "⚡ Basic — rules + SMTP verification (~5 sec/biz, free, <5% bounce)",
            "verified": "✅ Verified — rules + Haiku fallback + SMTP pattern testing (~6 sec/biz, ~$0.30/200, <2% bounce)",
            "deep": "🧠 Deep — 4 agents + Sonnet + SMTP + Haiku (~10 sec/biz, ~$2/200, <2% bounce)",
            "triangulation": "🎯 Triangulation (recommended) — 5 parallel agents + NPI + NeverBounce gate (~45s/biz, ~$5-6/100)",
        }[k],
        horizontal=False,
        index=3,  # default to Triangulation
    )
with mode_col2:
    parallelism = st.slider(
        "Parallel workers", 1, 12, 6,
        help="Higher = faster but more API pressure. 6 is safe for SearchApi.",
    )

if mode == "verified":
    cost = len(pending) * 0.0015  # ~$0.30 per 200 = $0.0015 per biz
    st.caption(f"💰 Verified mode estimated cost: ~${cost:.2f} for {len(pending)} businesses "
                "(Haiku fires on ~30% of businesses; SMTP verification is free)")
elif mode == "deep":
    cost = len(pending) * 0.02
    st.caption(f"💰 Deep mode estimated cost: ~${cost:.2f} for {len(pending)} businesses")
elif mode == "triangulation":
    cost = len(pending) * 0.055
    st.caption(
        f"💰 Triangulation mode estimated cost: ~${cost:.2f} for {len(pending)} businesses "
        "(~$0.05-0.06/biz: 2 SearchApi calls + 1 NeverBounce verify per business). "
        "Only the top candidate is NeverBounced — SMTP probes are free."
    )

# ── Start button ──
bc1, bc2 = st.columns([3, 1])
with bc1:
    start_clicked = st.button(
        f"🚀 Scrape {len(pending)} businesses in background",
        type="primary",
        disabled=bool(running_job) or not pending,
        help="Starts a background job. Navigate to any page while it runs — progress persists.",
    )
with bc2:
    if running_job:
        if st.button("🛑 Cancel job"):
            background_jobs.cancel(running_job["id"])
            time.sleep(1)
            st.rerun()


def _scrape_worker(biz, job_id):
    """Worker for one business. Runs scrape, scores, stores."""
    try:
        addr = biz.get("address", "") or biz.get("location", "")
        city = addr.split(",")[0].strip() if addr else ""
        business_type = biz.get("business_type", "") or ""
        phone = biz.get("phone", "") or ""

        if mode == "triangulation":
            # v3 parallel-agent pipeline: NPI + website + Google + press
            # + SMTP + NeverBounce gate. Lazy-imported so any issue in the
            # pipeline module surfaces at run-time, not page-load time.
            from src.email_scraper import scrape_with_triangulation
            result = scrape_with_triangulation(biz, use_neverbounce=True)
        elif mode == "deep":
            result = deep_scrape_business_emails(
                business_name=biz["business_name"],
                website=biz.get("website", ""),
                location=city,
                verify_with_mx=True,
                business_type=business_type,
                address=addr,
                phone=phone,
            )
        elif mode == "verified":
            result = scrape_business_emails(
                business_name=biz["business_name"],
                website=biz.get("website", ""),
                find_decision_makers=True,
                location=city,
                auto_verify=True,
                use_haiku_fallback=True,
                business_type=business_type,
                address=addr,
                phone=phone,
            )
        else:  # basic — still runs SMTP to prevent bounces, just skips Haiku
            result = scrape_business_emails(
                business_name=biz["business_name"],
                website=biz.get("website", ""),
                find_decision_makers=True,
                location=city,
                auto_verify=True,         # ALWAYS verify — prevents bounces
                use_haiku_fallback=False, # skip Haiku in basic mode
                business_type=business_type,
                address=addr,
                phone=phone,
            )

        storage.update_business_emails(biz["id"], result)

        # Compute lead quality score now that we have all the data
        fresh = storage.list_businesses(search_id=biz.get("search_id"))
        updated = next((b for b in fresh if b["id"] == biz["id"]), None)
        if updated:
            score = compute_lead_quality_score(updated)
            storage.update_lead_score(
                biz["id"], score["score"], score["tier"],
                all_emails=result.get("scraped_emails", []),
            )

        email = result.get("primary_email", "") or "(no email)"
        conf = result.get("confidence", "")
        return True, f"✓ {biz['business_name']} → {email} ({conf})"
    except Exception as e:
        return False, f"❌ {biz['business_name']}: {type(e).__name__}: {e}"


if start_clicked and pending:
    job_type = {
        "deep": "bulk_deep_scrape",
        "verified": "bulk_verified_scrape",
        "basic": "bulk_scrape",
        "triangulation": "bulk_triangulation_scrape",
    }[mode]
    job_id = background_jobs.start(
        job_type=job_type,
        items=pending,
        worker_fn=_scrape_worker,
        search_id=search_id,
        max_workers=parallelism,
        metadata={"mode": mode, "search_label": labels[search_id]},
    )
    st.success(f"🚀 Job started (id `{job_id[:8]}`) — you can navigate to any page.")
    time.sleep(1)
    st.rerun()


# ── Live job status ──
if running_job:
    st.divider()
    st.subheader(f"🟢 Job `{running_job['id'][:8]}` running")
    pct = int(100 * (running_job.get("progress", 0) or 0) /
               max(1, running_job.get("total", 0) or 1))
    jc1, jc2, jc3 = st.columns(3)
    jc1.metric("Progress", f"{running_job.get('progress', 0)} / {running_job.get('total', 0)}", f"{pct}%")
    jc2.metric("Succeeded", running_job.get("success_count", 0))
    jc3.metric("Errors", running_job.get("error_count", 0))
    st.progress(pct / 100)

    log_entries = []
    try:
        import json as _json
        log_entries = _json.loads(running_job.get("log_json") or "[]")
    except Exception:
        pass
    if log_entries:
        with st.expander(f"Live log ({len(log_entries)} entries)", expanded=True):
            for entry in log_entries[-15:][::-1]:
                st.caption(f"`{entry.get('ts', '')[11:19]}` {entry.get('msg', '')}")

    time.sleep(3)
    st.rerun()


# ── Results + ranking ──
st.divider()
st.subheader("🏆 Ranked leads")

if not scraped:
    st.info("No scraped businesses yet. Run a bulk scrape above.")
    st.stop()

# ── Rescore-only button (fixes empty Score/Tier without re-scraping) ──
rs_col1, rs_col2 = st.columns([3, 1])
with rs_col1:
    missing_scores = [b for b in scraped
                       if not b.get("lead_quality_score")
                       and b.get("confidence")]
    if missing_scores:
        st.caption(
            f"⚠️ {len(missing_scores)} businesses have no Score/Tier "
            "(scraped before scoring was added). Click Rescore to fix."
        )
with rs_col2:
    if st.button(f"🔄 Rescore {len(missing_scores)} businesses",
                  disabled=not missing_scores):
        progress = st.progress(0)
        for i, b in enumerate(missing_scores):
            s = compute_lead_quality_score(b)
            storage.update_lead_score(b["id"], s["score"], s["tier"])
            progress.progress((i + 1) / len(missing_scores))
        st.success(f"✅ Rescored {len(missing_scores)} businesses")
        st.rerun()

# Compute fresh scores for all scraped businesses (recalculated each load
# so the UI always reflects current data)
ranked = rank_businesses(list(scraped))

# ── Filters ──
f1, f2, f3, f4 = st.columns(4)
with f1:
    min_score = st.number_input("Min score", 0, 100, 50, step=5)
with f2:
    tier_filter = st.multiselect("Tiers", ["A", "B", "C", "D", "F"],
                                   default=["A", "B"])
with f3:
    conf_filter = st.multiselect(
        "Email confidence",
        ["high", "medium", "low"],
        default=["high", "medium"],
    )
with f4:
    min_rating = st.number_input("Min Google rating", 0.0, 5.0, 0.0, step=0.1)

filtered = [
    b for b in ranked
    if b.get("lead_quality_score", 0) >= min_score
    and b.get("lead_tier", "") in tier_filter
    and (b.get("confidence", "") in conf_filter or not b.get("confidence"))
    and (b.get("rating") or 0) >= min_rating
]

# If the combined filter knocks everything out, show which knob dropped what
# so the user knows which threshold to loosen. Otherwise "No leads pass"
# leaves them guessing between 3 filters.
if ranked and not filtered:
    dropped_score = sum(1 for b in ranked if b.get("lead_quality_score", 0) < min_score)
    dropped_tier = sum(1 for b in ranked if b.get("lead_tier", "") not in tier_filter)
    dropped_conf = sum(
        1 for b in ranked
        if b.get("confidence") and b.get("confidence", "") not in conf_filter
    )
    dropped_rating = sum(1 for b in ranked if (b.get("rating") or 0) < min_rating)
    st.caption(
        f"Filters dropped: score&lt;{min_score} · **{dropped_score}** · "
        f"tier not in {tier_filter} · **{dropped_tier}** · "
        f"confidence not in {conf_filter} · **{dropped_conf}** · "
        f"rating&lt;{min_rating} · **{dropped_rating}**. "
        f"(Businesses may be dropped by multiple filters.)"
    )

# ── Pick top N ──
top_col1, top_col2 = st.columns([3, 1])
with top_col1:
    n_filtered = len(filtered)
    # Streamlit's slider rejects max < min. When fewer than 11 leads match
    # the filter there's nothing to pick — show an info message instead.
    if n_filtered <= 10:
        top_n = n_filtered
        if n_filtered > 0:
            st.info(f"Only {n_filtered} lead(s) pass the filter — scraping all of them.")
        else:
            st.info("No leads pass the current filter. Widen the filter above to pick a top-N.")
    else:
        slider_max = min(200, n_filtered)
        slider_default = min(50, n_filtered)
        top_n = st.slider("Pick top N", 10, slider_max, slider_default)
with top_col2:
    st.metric("Passing filter", len(filtered))

top_picks = filtered[:top_n]

# ── Display ──
if top_picks:
    # Build the FULL export schema once (one source of truth with the
    # Export CSV page). The table view below hides most columns; the CSV
    # download includes every field so the audit tool + stakeholder
    # consumers get the same data regardless of which page exported.
    show_evidence = st.toggle(
        "Show verification evidence columns (SMTP / WHOIS / NPI / Pattern)",
        value=False,
        help="Tick columns derived from the triangulation evidence trail. "
             "Hidden by default; always included in the CSV download.",
    )
    full_rows = build_rows(top_picks, include_evidence=show_evidence)
    df_full = pd.DataFrame(full_rows)

    # On-screen: narrow, glanceable subset. Appends any evidence tick
    # columns when the operator opted in.
    display_cols = list(BULK_DISPLAY_COLUMNS)
    if show_evidence:
        for col in ("SMTP ✓", "WHOIS ✓", "NPI/Pattern ✓"):
            if col in df_full.columns and col not in display_cols:
                display_cols.insert(3, col)
    df_display = df_full[[c for c in display_cols if c in df_full.columns]]
    st.dataframe(
        df_display, use_container_width=True, hide_index=True,
        column_config={
            "Score": st.column_config.ProgressColumn(
                "Score", min_value=0, max_value=100, format="%d"
            ),
            "Website": st.column_config.LinkColumn("Website"),
            "Email Source": st.column_config.TextColumn(
                "Email Source",
                help="How the email was found — e.g. triangulated pattern, "
                     "scraped from website, industry prior, fallback.",
                width="large",
            ),
        },
    )

    # ── Export selected ──
    # CSV uses the FULL schema — not the curated display subset — so
    # downstream consumers (audit tool, stakeholders) get every field
    # we know about the lead, same as the Export CSV page.
    st.divider()
    import io
    # Always include evidence columns in CSV regardless of the UI toggle
    csv_rows = build_rows(top_picks, include_evidence=True)
    df_csv = pd.DataFrame(csv_rows)
    # Enforce canonical column order for audit-tool compatibility
    ordered = [c for c in EXPORT_COLUMNS if c in df_csv.columns]
    remaining = [c for c in df_csv.columns if c not in ordered]
    df_csv = df_csv[ordered + remaining]
    buf = io.StringIO()
    df_csv.to_csv(buf, index=False)
    c_csv, c_log = st.columns(2)
    with c_csv:
        st.download_button(
            f"📥 Download top {len(top_picks)} as CSV "
            f"({len(df_csv.columns)} columns)",
            data=buf.getvalue(),
            file_name=f"top_{len(top_picks)}_leads_search_{search_id}.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True,
        )
    with c_log:
        # Decision log — per-business: score breakdown, gate decision, all
        # candidates, triangulation evidence. Grep a bad lead, see why.
        import json as _json
        from src.decision_log import build_search_decision_log
        try:
            log = build_search_decision_log(search_id)
            st.download_button(
                "🧠 Download decision log (.json)",
                data=_json.dumps(log, indent=2, default=str),
                file_name=f"decision_log_search_{search_id}.json",
                mime="application/json",
                use_container_width=True,
                help="Full decision tree per business: scoring breakdown, "
                     "gate decision, all candidate emails, triangulation "
                     "evidence, agents run. Debug bad results by grepping "
                     "the JSON.",
            )
        except Exception as e:
            st.caption(f"⚠️ Decision log unavailable: {e}")
else:
    st.info("No businesses match the current filters. Lower the thresholds above.")

# ── Score breakdown explainer ──
with st.expander("ℹ️ How lead scores are calculated"):
    st.markdown("""
    Each business gets a 0-100 score combining five signals:

    | Component | Max | What it measures |
    |---|---|---|
    | **Email confidence** | 40 | HIGH=40, MEDIUM=25, LOW=10 (+ verification bonus) |
    | **Google rating** | 20 | 4.8+=20, 4.5+=17, 4.0+=13, 3.5+=8 |
    | **Review count** | 15 | 500+=15, 200+=13, 100+=11, 50+=8 |
    | **Website present** | 10 | Real domain=10, social-only=4 |
    | **Decision maker** | 15 | Name+title=15, name only=10, title only=4 |

    **Tiers:** A=80+, B=65+, C=50+, D=35+, F<35
    """)
