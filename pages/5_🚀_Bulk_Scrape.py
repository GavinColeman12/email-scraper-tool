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

# ── NeverBounce credit check ──
# Warns before the run starts if NB is out of credits — prevents
# burning 15+ minutes on a bulk scrape that can't verify anything
# and silently produces "unknown" for every candidate.
try:
    from src.neverbounce import get_account_info, is_available as _nb_available
    if _nb_available():
        _nb_info = get_account_info()
        _credits = _nb_info.get("credits_info", {}) if isinstance(_nb_info, dict) else {}
        _total_credits = (
            int(_credits.get("paid_credits_remaining") or 0)
            + int(_credits.get("free_credits_remaining") or 0)
        )
        if _total_credits == 0:
            st.error(
                "⚠️ **NeverBounce account has 0 credits remaining.** "
                "Volume/Triangulation modes will run but every candidate will return "
                "`unknown` — you'll see lots of 🟡 MEDIUM instead of 🟢 HIGH. "
                "[Top up at neverbounce.com](https://app.neverbounce.com/account/billing) "
                "before starting a bulk run."
            )
        elif _total_credits < 100:
            st.warning(
                f"⚠️ NeverBounce has only **{_total_credits}** credits left — "
                f"a 200-biz volume run uses ~600-800. Top up if needed."
            )
except Exception:
    pass  # never block the UI on an NB status check

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
# Three distinct states (previously collapsed into "Pending"):
#   not_scraped     — never processed (scraped_at is NULL). These are
#                     the only ones that actually need a scrape run.
#   scraped_no_email — processed, pipeline exhausted without finding a
#                     deliverable email (DM rejected all NB probes, or
#                     every bucket-D pattern bounced, etc.). NOT stuck
#                     and NOT retryable — the domain genuinely doesn't
#                     accept mail at any pattern we tried.
#   with_email      — has primary_email.
not_scraped = [b for b in businesses if not b.get("scraped_at") and b.get("website")]
scraped_no_email = [
    b for b in businesses
    if b.get("scraped_at") and not b.get("primary_email") and b.get("website")
]
with_email = [b for b in businesses if b.get("primary_email")]
no_website = [b for b in businesses if not b.get("website")]

# Backwards-compat alias — the ranking section below uses `scraped` to
# mean "businesses with a primary_email that can be ranked as leads".
# Renamed the counter variable above for UX clarity but kept this alias
# so the rest of the page keeps working.
scraped = with_email

# "pending" for the scrape button = only biz that actually need a run.
# Re-scraping a scraped_no_email biz produces the same result unless
# the logic / cache has changed, so we don't auto-include them — but
# we surface a toggle below to let the operator force a retry.
pending = not_scraped

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total", len(businesses))
c2.metric("Not scraped yet", len(not_scraped),
          help="Businesses with a website that haven't been processed yet. "
               "These are what the Scrape button will run.")
c3.metric("With email", len(with_email),
          help="Businesses where the pipeline produced a deliverable email.")
c4.metric("No email found", len(scraped_no_email),
          help="Processed, but pipeline exhausted all patterns without finding "
               "a deliverable address (typical for domains with strict "
               "mailbox enforcement). Not 'stuck' — these are done. "
               "Enable 'Retry exhausted' below to re-scrape them after a logic upgrade.")
c5.metric("No website", len(no_website),
          help="Businesses Google Maps returned without a website. Can't be scraped.")

# Offer retry for the exhausted cases — useful after pipeline upgrades
# when the new logic might find something the old one missed.
if scraped_no_email:
    retry_exhausted = st.toggle(
        f"🔁 Retry {len(scraped_no_email)} 'no email found' businesses "
        f"(re-scrape with current logic — may recover some after pipeline fixes)",
        value=False,
        help="Use this after landing a pipeline improvement to give previously-"
             "exhausted businesses another chance. Cost: same as a fresh scrape "
             "for each, but most of the crawl data is cached so it's fast.",
    )
    if retry_exhausted:
        pending = not_scraped + scraped_no_email

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
    # Volume is the default AND recommended mode. NPI lookup is now
    # ported into volume for medical verticals (free), and the
    # "Rescue empties" toggle below offers the combined owner+press
    # SearchApi path for hard cases. Triangulation is retained but
    # deprecated — 5-6× the cost for marginal extra coverage.
    mode = st.radio(
        "Mode",
        ["volume", "basic", "verified", "deep", "triangulation"],
        format_func=lambda k: {
            "volume": "🚀 **Volume** (RECOMMENDED) — deep crawl + Wayback + NPI + selective NB (~30s/biz, <$4/200, never picks info@)",
            "basic": "⚡ Basic — rules + SMTP verification (~5 sec/biz, free, <5% bounce)",
            "verified": "✅ Verified — rules + Haiku fallback + SMTP pattern testing (~6 sec/biz, ~$0.30/200)",
            "deep": "🧠 Deep — 4 agents + Sonnet + SMTP + Haiku (~10 sec/biz, ~$2/200)",
            "triangulation": "⚠️ Triangulation (DEPRECATED) — 5 parallel agents, ~$5-6/100, 5× volume's cost for marginal lift",
        }[k],
        horizontal=False,
        index=0,  # default to Volume
    )
with mode_col2:
    parallelism = st.slider(
        "Parallel workers", 1, 12, 10,
        help="Higher = faster but more API pressure. 6 is safe for SearchApi.",
    )

# Show deprecation banner when user selects triangulation
if mode == "triangulation":
    st.warning(
        "⚠️ **Triangulation is deprecated.** Volume mode now includes "
        "NPI verification for medical verticals (free) and has a "
        "'Rescue empties' toggle that runs the combined owner+press "
        "SearchApi only on rows volume couldn't solve. That gives you "
        "~95% of triangulation's coverage at ~20% of the cost. Keep "
        "triangulation only for replay-compatibility with old runs."
    )

# Volume-specific: rescue-empty SearchApi opt-in
rescue_empties = False
if mode == "volume":
    rescue_empties = st.checkbox(
        "🔍 Rescue empties with SearchApi (~$0.010 per empty biz)",
        value=False,
        help="When volume's crawl + Wayback + NPI + LinkedIn all produce "
             "zero DM candidates for a biz, optionally fire the combined "
             "owner+press SearchApi to find the founder via press mentions "
             "/ third-party listings. Only hits the ~20% of rows volume "
             "couldn't solve, so on a 200-biz campaign this adds ~$0.40 "
             "rather than triangulation's $10+. Recommended ON for "
             "hard-to-reach verticals (legal / construction / manufacturing).",
    )

if mode == "verified":
    # Cost: Haiku fires ~30% of biz ($0.001 × 0.30 = $0.0003) + SMTP probes (free)
    # Avg: $0.0003/biz · max (Haiku every biz): $0.002/biz
    cost_avg = len(pending) * 0.0005
    cost_max = len(pending) * 0.002
    st.caption(
        f"💰 **Verified mode:** ~${cost_avg:.2f} typical · ${cost_max:.2f} worst-case "
        f"for {len(pending)} businesses. Haiku name-filter fires on ~30% of biz; "
        "SMTP verification is free."
    )
elif mode == "deep":
    # 4 Sonnet agents per biz (~$0.015-0.02 each) + SMTP + Haiku
    cost_avg = len(pending) * 0.015
    cost_max = len(pending) * 0.025
    st.caption(
        f"💰 **Deep mode:** ~${cost_avg:.2f} typical · ${cost_max:.2f} worst-case "
        f"for {len(pending)} businesses (Sonnet-powered 4-agent research)."
    )
elif mode == "triangulation":
    # Triangulation per biz:
    #   Phase 1: combined owner+press search ($0.005) + optional LinkedIn ($0.005, ~40%)
    #   Phase 3B: colleague emails ($0.005 × up to 3, ~20% of biz)
    #   Phase 6: NeverBounce (up to 4 × $0.003 = $0.012, avg ~$0.006)
    #   Phase 1.5: Haiku classifier (~$0.002 cached, ~$0.002 fresh)
    # Avg: ~$0.020-0.025/biz · Worst case: ~$0.050/biz
    cost_avg = len(pending) * 0.025
    cost_max = len(pending) * 0.050
    st.caption(
        f"💰 **Triangulation mode:** ~${cost_avg:.2f} typical · ${cost_max:.2f} worst-case "
        f"for {len(pending)} businesses (~${cost_avg/max(1,len(pending))*1000:.0f}/1000 typical). "
        "Full 7-phase pipeline: combined owner search + press + LinkedIn fallback + "
        "colleague email harvest + NeverBounce walk. SMTP probes + WHOIS + NPI are free."
    )
elif mode == "volume":
    # Cost composition per biz:
    #   Crawl + Wayback + Haiku name-filter: free (bandwidth + $0.001 cached)
    #   LinkedIn fallback: $0.005, fires on ~30% of biz → $0.0015 avg
    #   NeverBounce: up to 4 calls × $0.003 = $0.012 max,
    #                ~2 calls avg = $0.006
    #   Budget cap: $0.025/biz hard ceiling
    # Typical: $0.008-0.010/biz  ·  Worst case: $0.025/biz
    avg_per_biz = 0.009
    max_per_biz = 0.025
    cost_avg = len(pending) * avg_per_biz
    cost_max = len(pending) * max_per_biz
    st.caption(
        f"💰 **Volume mode estimated cost:** ~${cost_avg:.2f} typical "
        f"(${cost_max:.2f} hard cap) for {len(pending)} businesses "
        f"· ${avg_per_biz*1000:.0f}/1000 typical · "
        f"${max_per_biz*1000:.0f}/1000 worst-case.\n\n"
        "**Per-biz breakdown:** crawl + Wayback + Haiku classifier = ~$0 · "
        "LinkedIn fallback (~$0.005, fires when crawl finds no DM, ~30% of biz) · "
        "NeverBounce (~$0.003 × 2-4 calls = $0.006-0.012 typical). "
        "Hard per-biz cap: $0.025.\n\n"
        "**Generic inboxes — info@, contact@, hello@, smile@, etc. — are never picked.** "
        "Decision makers first; industry prior (law → first.last, dental → first.last) "
        "is the last resort."
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

        if mode == "volume":
            # Volume Mode: deep crawl + Wayback + NPI (medical) +
            # selective NB. ~10× cheaper than triangulation. Never
            # picks generic inboxes. rescue_empties adds SearchApi
            # on volume_empty rows only.
            from src.volume_mode import scrape_volume
            from src.volume_mode.pipeline import volume_result_to_scrape_result
            vres = scrape_volume(
                biz, use_neverbounce=True,
                rescue_empties_with_searchapi=rescue_empties,
            )
            result = volume_result_to_scrape_result(vres, biz)
        elif mode == "triangulation":
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
        "volume": "bulk_volume_scrape",
    }[mode]

    # Volume mode tracks a shared cost cap across the run — reset the
    # counter at the start of every new job so yesterday's spend doesn't
    # gate today's run.
    if mode == "volume":
        from src.volume_mode.pipeline import reset_run_budget
        reset_run_budget(25.0)
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

# ── Warmup banner — shows daily cap + today's send count for new senders ──
try:
    from src.send_safety import (
        sender_first_send_date, recommended_daily_cap, sent_today_count,
    )
    first_send = sender_first_send_date()
    cap_info = recommended_daily_cap(first_send)
    sent_today = sent_today_count()
    remaining = max(0, cap_info["cap"] - sent_today)
    over_cap = sent_today > cap_info["cap"]

    wb1, wb2, wb3 = st.columns([2, 2, 3])
    wb1.metric(
        "Today's send cap",
        f"{cap_info['cap']}/day",
        help=(
            f"Week {cap_info['week']} of warmup · {cap_info['stage']}.\n\n"
            "Warmup protects sender reputation by gradually increasing "
            "daily volume. Sending over the cap on a new domain risks "
            "ISP throttling + bounces that damage deliverability for "
            "months."
        ),
    )
    wb2.metric(
        "Sent today",
        f"{sent_today}",
        delta=(f"{remaining} remaining" if not over_cap
               else f"{sent_today - cap_info['cap']} OVER cap"),
        delta_color=("normal" if not over_cap else "inverse"),
    )
    if over_cap:
        wb3.error(
            "⚠️ Over your warmup cap today. Pause sending until tomorrow — "
            "pushing past the cap on a new sender domain is the single "
            "biggest cause of sub-0.3% bounce targets failing in the first "
            "month."
        )
    elif cap_info["next_bump_in_days"] is not None and cap_info["next_bump_in_days"] <= 7:
        wb3.info(
            f"📅 Cap bumps in {cap_info['next_bump_in_days']} days "
            f"(stage: {cap_info['stage']})."
        )
except Exception:
    pass

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
        ["high", "review", "medium", "low"],
        default=["high"],  # default to verified-only — user explicitly opts
                           # into review/medium/low for wider nets
        help="high=🟢 NB-valid · review=🟣 NB-unknown (manual check first) "
             "· medium=🟡 catchall · low=🔴 industry-prior guess",
    )
with f4:
    min_rating = st.number_input("Min Google rating", 0.0, 5.0, 0.0, step=0.1)

# ── Strict "Send Safe" gate — enterprise-grade <0.3% bounce target ──
# Composes: NB=valid + MX + fresh verdict + no domain bounce history +
# rating ≥ 3.0 + pipeline safe_to_send flag. When ON, hides every row
# that fails any of these. See src/send_safety.py for the full logic.
ss1, ss2 = st.columns([3, 2])
with ss1:
    send_safe_only = st.toggle(
        "🛡️ Send-safe only (target <0.3% bounce)",
        value=True,
        help=(
            "Strict gate that composes every deliverability signal:\n"
            "• NB verdict must be 'valid' (rejects catchall/unknown/invalid)\n"
            "• NB verdict must be <14 days old (rescrape stale rows)\n"
            "• Domain must not have prior bounces in your send history\n"
            "• Google rating ≥ 3.0 with ≥1 review\n"
            "• Pipeline safe_to_send flag set\n\n"
            "Turn OFF for wider nets at higher bounce risk. Recommended "
            "ON for new sender domains and enterprise-grade bounce targets."
        ),
    )
with ss2:
    permissive_mode = st.checkbox(
        "Permissive (skip freshness + rating gates)",
        value=False,
        disabled=not send_safe_only,
        help="Keep NB-valid + no-bounce-history gates but relax the "
             "stale-NB and rating checks. Expect ~1% bounce at this setting.",
    )

filtered = [
    b for b in ranked
    if b.get("lead_quality_score", 0) >= min_score
    and b.get("lead_tier", "") in tier_filter
    and (b.get("confidence", "") in conf_filter or not b.get("confidence"))
    and (b.get("rating") or 0) >= min_rating
]

# Apply the strict send-safety gate on top of the regular filters
if send_safe_only:
    try:
        from src.send_safety import (
            is_safe_to_send, domains_with_bounces, mark_duplicate_emails,
        )
        bounce_domains = domains_with_bounces()
        # Detect within-search duplicates FIRST so the name-match gate
        # and NB gate don't silently pass two rows pointing at the
        # same mailbox (classic chain-franchise failure mode — same
        # corporate contact lives in N rows, sending to all N = spam
        # signal to ESPs).
        dup_index = mark_duplicate_emails(filtered)
        safe_filtered = []
        unsafe_count = 0
        unsafe_reasons_tally: dict[str, int] = {}
        for b in filtered:
            # Duplicate check: keep the first occurrence, flag later ones
            if dup_index.get(b.get("id"), 0) > 0:
                unsafe_count += 1
                unsafe_reasons_tally["duplicate email within search"] = (
                    unsafe_reasons_tally.get("duplicate email within search", 0) + 1
                )
                continue
            safe, reasons = is_safe_to_send(
                b, domain_bounce_set=bounce_domains,
                permissive=permissive_mode,
            )
            if safe:
                safe_filtered.append(b)
            else:
                unsafe_count += 1
                for r in reasons:
                    # Bucket reasons by their prefix so the UI
                    # summary stays readable (e.g. count all
                    # "NB catchall" regardless of the full text).
                    key = r.split(" — ")[0].split(" (")[0].strip()
                    unsafe_reasons_tally[key] = unsafe_reasons_tally.get(key, 0) + 1
        filtered = safe_filtered
        if unsafe_count:
            tally_str = " · ".join(
                f"**{c}** × {reason}"
                for reason, c in sorted(
                    unsafe_reasons_tally.items(),
                    key=lambda x: -x[1],
                )[:5]
            )
            st.caption(
                f"🛡️ Send-safe gate held back **{unsafe_count}** rows. "
                f"Top reasons: {tally_str}"
            )

            # ── Rescue action: retry NB + try more DM patterns ──
            # For every row the gate held back, attempt to upgrade:
            #   - NB-unknown → re-verify (often a transient API issue)
            #   - Name-mismatch → try 15+ additional DM pattern guesses
            # Only offer when the bounce-history + NB-invalid rows are
            # excluded (those are unrecoverable).
            from src.send_safety import classify_for_send
            review_biz = [
                b for b in ranked
                if classify_for_send(
                    b, domain_bounce_set=bounce_domains,
                    permissive=permissive_mode,
                ) in ("review", "reverify")
            ]
            if review_biz:
                est_cost = min(len(review_biz) * 0.009, 2.0)

                # Show what we LEARNED from send-safe history — this
                # is what drives rescue's pattern priority per vertical.
                try:
                    from src.learned_priors import summarize_for_ui
                    lp = summarize_for_ui()
                    if lp["total_samples"] >= 10:
                        with st.expander(
                            f"📊 Learned patterns from your "
                            f"{lp['total_samples']} NB-valid sends — "
                            "drives rescue priority per vertical + CMS"
                        ):
                            st.markdown("**By industry**")
                            verticals = lp["verticals_with_data"]
                            if verticals:
                                for v, info in sorted(
                                    verticals.items(),
                                    key=lambda x: -x[1]["samples"],
                                ):
                                    top = " · ".join(
                                        f"**{p}** ({pct:.0f}%)"
                                        for p, _, pct in info["top_3"]
                                    )
                                    st.caption(
                                        f"**{v}** ({info['samples']} "
                                        f"samples) — {top}"
                                    )
                            else:
                                st.caption(
                                    "No vertical has 5+ samples yet — "
                                    "using hardcoded fallback order "
                                    "(flast, first, first.last)."
                                )
                            # CMS × pattern distribution (new — populated
                            # as volume mode scrapes tag each row with
                            # the detected CMS)
                            cms_data = lp.get("cms_with_data") or {}
                            if cms_data:
                                st.markdown("**By CMS platform**")
                                for cms, info in sorted(
                                    cms_data.items(),
                                    key=lambda x: -x[1]["samples"],
                                ):
                                    top = " · ".join(
                                        f"**{p}** ({pct:.0f}%)"
                                        for p, _, pct in info["top_3"]
                                    )
                                    # Show NB verdict distribution too
                                    nb_dist = info.get("nb_verdicts") or {}
                                    nb_str = " · ".join(
                                        f"{v}:{c}" for v, c in sorted(
                                            nb_dist.items(), key=lambda x: -x[1],
                                        )
                                    )
                                    st.caption(
                                        f"**{cms}** ({info['samples']} "
                                        f"valid-samples) — {top}  \n"
                                        f"  NB verdicts on this CMS: {nb_str}"
                                    )
                            else:
                                st.caption(
                                    "CMS data not yet captured — re-run "
                                    "a scrape to start tagging rows by "
                                    "platform."
                                )
                except Exception:
                    pass

                rc1, rc2 = st.columns([3, 2])
                with rc1:
                    st.caption(
                        f"💡 **{len(review_biz)} rows are in review or "
                        f"reverify** — rescue tries to upgrade them by "
                        f"re-NBing the email + testing up to 3 of the "
                        f"highest-probability DM patterns "
                        f"(learned per vertical). Est. cost: "
                        f"**~${est_cost:.2f}** (hard cap $2 per batch)."
                    )
                with rc2:
                    if st.button(
                        f"🚑 Rescue {len(review_biz)} review rows",
                        help="Max 3 NB calls per row (~$0.009). Tries the "
                             "top-3 highest-probability DM patterns not "
                             "already tested: {first}.{last}, {f}{last}, "
                             "{first}@. Dental/medical swaps slots 2-3 "
                             "to dr.{last} + dr{last}. Budget: $0.009/row, "
                             "$2 total cap.",
                    ):
                        from src.review_rescue import bulk_rescue
                        prog = st.progress(0)
                        status_box = st.empty()

                        def _progress(i, total, result):
                            prog.progress(i / total)
                            status_box.write(
                                f"**{i}/{total}** · {result.status} "
                                f"· {result.reason[:80]}"
                            )

                        with st.spinner("Running rescue pass…"):
                            summary = bulk_rescue(
                                review_biz,
                                total_budget_usd=2.0,
                                progress_cb=_progress,
                            )
                        # Persist upgrades via the focused helper —
                        # update_business_emails() is a full-row
                        # overwrite that would wipe contact_name,
                        # evidence trail, triangulation data, etc.
                        # apply_rescue_upgrade touches only the 5
                        # fields that actually change on a rescue.
                        updated = 0
                        persist_errors: list[str] = []
                        for u in summary["upgraded"]:
                            try:
                                storage.apply_rescue_upgrade(
                                    business_id=u["biz_id"],
                                    new_email=u["new_email"],
                                    new_nb_result=u["new_nb_result"],
                                    confidence="high",
                                )
                                updated += 1
                            except Exception as e:
                                persist_errors.append(
                                    f"biz {u['biz_id']}: {type(e).__name__}: {e}"
                                )

                        prog.empty()
                        status_box.success(
                            f"🚑 Rescue done — "
                            f"**{updated} persisted** / "
                            f"{len(summary['upgraded'])} upgrades · "
                            f"**{len(summary['exhausted'])}** exhausted · "
                            f"**{len(summary['skipped'])}** skipped · "
                            f"spent **${summary['total_cost_usd']:.3f}**"
                            + (" (hit budget cap)" if summary["stopped_early"] else "")
                        )
                        if persist_errors:
                            with st.expander(
                                f"⚠️ {len(persist_errors)} persist errors"
                            ):
                                for err in persist_errors:
                                    st.text(err)
                        if summary["upgraded"]:
                            with st.expander(
                                f"✅ Show {len(summary['upgraded'])} upgraded rows"
                            ):
                                st.dataframe(
                                    summary["upgraded"],
                                    use_container_width=True, hide_index=True,
                                )
                        st.rerun()
    except Exception as e:
        st.warning(f"Send-safe gate failed to apply: {e}")

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
    Each business gets a 0-100 score combining four signals. Email
    verifiability and decision-maker identity dominate — they're 80% of
    the score. Google rating and review count are tiebreakers.

    | Component | Max | What it rewards |
    |---|---|---|
    | **Email verifiability** | 40 | NB-valid scraped DM = 40 · NB-valid triangulated pattern = 38 · NB-valid industry-prior guess = 36 · NB-valid scraped non-DM = 30 · catchall/unknown/untested = 10-24 · NB-invalid = 0 |
    | **Decision maker** | 40 | +10 name present · +10 executive title (Founder, CEO, Owner, Managing Partner, …) · +10 last name matches business name (e.g. Weaver → Weaver Law) · +10 LinkedIn-sourced · +5 cross-verified across agents |
    | **Review count** | 15 | 500+=15 · 200+=13 · 100+=11 · 50+=8 · 10+=5 |
    | **Google rating** | 5 | 4.8+=5 · 4.5+=4 · 4.0+=3 · 3.5+=2 |

    **Tiers:** A=80+, B=65+, C=50+, D=35+, F<35

    **Confidence tiers (volume mode):**
    - 🟢 **high / volume_verified** — NB returned VALID. Deliverable confirmed. **Safe to send.**
    - 🟣 **review / volume_review** — NB returned UNKNOWN (server refused or out of credits). **Do NOT auto-send.** Re-verify manually or top up NB credits.
    - 🟡 **medium / volume_scraped** — catchall domain (deliverable-looking but mailbox may not exist).
    - 🔴 **low / volume_guess** — industry-prior guess, not NB-verified.
    - ⚫ **volume_empty** — no plausible email found.
    """)
