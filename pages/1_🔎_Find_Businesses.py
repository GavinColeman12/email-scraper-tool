import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from src import storage
from src.maps_search import search_businesses, estimate_cost, SearchError

st.set_page_config(page_title="Find Businesses", page_icon="🔎", layout="wide")
st.title("🔎 Find Businesses on Google Maps")

storage.init_db()

st.markdown(
    "Enter a business type and location. We'll search Google Maps and save "
    "everything to a new search so you can scrape emails next."
)

with st.form("search_form"):
    c1, c2, c3 = st.columns([2, 2, 1])
    query = c1.text_input("Business type / search query",
                           placeholder="e.g. dental clinic, law firm, restaurant")
    location = c2.text_input("Location (city, state)",
                              placeholder="e.g. Manhattan NYC, Brooklyn NY")
    max_results = c3.slider("Max results", 10, 250, 100,
                             help="Google Maps usually caps a single query around 20-60 results. "
                                  "The scraper automatically tries synonym variants (e.g. dentist → "
                                  "dental clinic → dental practice) to cross that cap. Top out at "
                                  "~200 for most business types.")

    skip_seen = st.checkbox(
        "🧹 Skip businesses I've already seen (dedupe by Google place_id)",
        value=True,
        help="Checks all past searches and hides any businesses already in your DB. "
             "Lets you run the same query repeatedly and always see fresh results."
    )

    est = estimate_cost(max_results, query=query)
    if est["variants"] > 1:
        st.caption(
            f"💰 **Estimated cost:** ~${est['avg_usd']:.2f} typical, "
            f"${est['max_usd']:.2f} worst-case "
            f"(~{est['avg_calls']}-{est['max_calls']} API calls · "
            f"{est['variants']} query variants will fan out: the scraper tries "
            f"'{query}', then synonyms until it hits {max_results} unique results)"
        )
    else:
        st.caption(
            f"💰 **Estimated cost:** ~${est['avg_usd']:.2f} "
            f"({est['avg_calls']} API calls — no synonym expansion for this query)"
        )

    submitted = st.form_submit_button("🔍 Search Google Maps", type="primary")

if submitted:
    if not query:
        st.error("Please enter a search query.")
    else:
        with st.spinner(f"Searching Google Maps for '{query}' in '{location}'..."):
            try:
                results = search_businesses(query, location, max_results=max_results)
            except SearchError as e:
                st.error(f"Search failed: {e}")
                results = []
            except Exception as e:
                st.error(f"Unexpected error: {e}")
                results = []

        if results:
            total_found = len(results)
            if skip_seen:
                known_ids = storage.existing_place_ids()
                before = len(results)
                results = [r for r in results if r.get("place_id") not in known_ids]
                skipped = before - len(results)
                if skipped > 0:
                    st.info(
                        f"🧹 Skipped **{skipped}** businesses already in your database — "
                        f"showing **{len(results)}** new ones (of {total_found} found)."
                    )

            # If we returned significantly fewer than the user asked for,
            # tell them why + how to get more
            if total_found < max_results * 0.6:
                st.warning(
                    f"⚠️ Google Maps only returned **{total_found}** unique results "
                    f"for this query (you asked for {max_results}). "
                    f"Try:\n"
                    f"- A broader business-type synonym (e.g. `dentist`, `dental clinic`, "
                    f"`dental office` instead of just `dental`)\n"
                    f"- A larger geographic area (e.g. `New York NY` instead of just "
                    f"`Manhattan`)\n"
                    f"- Running multiple searches in different neighborhoods "
                    f"(e.g. `Brooklyn NY`, `Queens NY`)"
                )

            if results:
                st.success(f"Found **{len(results)}** businesses.")
                st.session_state["last_results"] = results
                st.session_state["last_query"] = query
                st.session_state["last_location"] = location
            else:
                st.warning(
                    "All businesses from this search are already in your database. "
                    "Try a different query, location, or uncheck the dedupe box."
                )

# ── Show results + save ──
results = st.session_state.get("last_results", [])
if results:
    st.divider()
    st.subheader("Preview")

    # ── Pre-save filters (hide low-quality leads BEFORE they hit the
    # scrape queue — saves API spend on businesses you'd never email) ──
    with st.expander("🔍 Quality filters (applied before save)", expanded=True):
        fc1, fc2, fc3, fc4 = st.columns([1, 1, 1, 1])
        min_reviews = fc1.number_input(
            "Min reviews", min_value=0, max_value=100000, value=0, step=10,
            help="Only include businesses with at least this many Google reviews. "
                 "Strong signal of real, active business. 50+ is a good baseline.",
        )
        min_rating = fc2.number_input(
            "Min rating", min_value=0.0, max_value=5.0, value=0.0, step=0.1,
            format="%.1f",
            help="Only include businesses with at least this star rating. "
                 "4.0+ filters out problem businesses that won't convert.",
        )
        max_rating = fc3.number_input(
            "Max rating", min_value=0.0, max_value=5.0, value=5.0, step=0.1,
            format="%.1f",
            help="Upper bound. Useful when you want mid-tier businesses that have "
                 "room for improvement (e.g. 4.0-4.6).",
        )
        require_website = fc4.checkbox(
            "Must have website", value=False,
            help="Emails can only be scraped from businesses with websites. "
                 "Filter them out here instead of saving empties.",
        )

    def _passes_filters(b):
        rev = b.get("review_count") or 0
        rat = b.get("rating") or 0
        if rev < min_reviews:
            return False
        if rat < min_rating or rat > max_rating:
            return False
        if require_website and not b.get("website"):
            return False
        return True

    all_results = results
    results = [b for b in all_results if _passes_filters(b)]
    hidden = len(all_results) - len(results)
    if hidden > 0:
        st.caption(
            f"🔍 Filters hide **{hidden}** of {len(all_results)} results "
            f"(min {min_reviews} reviews, rating {min_rating:.1f}–{max_rating:.1f}"
            f"{', website required' if require_website else ''}). "
            "Adjust above to widen."
        )

    if not results:
        st.warning("No businesses match the current filters. Relax the thresholds above.")
        st.stop()

    df = pd.DataFrame([{
        "Include": True,
        "Name": b.get("business_name"),
        "Rating": b.get("rating"),
        "Reviews": b.get("review_count"),
        "Website": b.get("website") or "—",
        "Address": b.get("address"),
        "Phone": b.get("phone") or "—",
        "_place_id": b.get("place_id"),
    } for b in results])

    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Include": st.column_config.CheckboxColumn("✓", default=True),
            "Website": st.column_config.LinkColumn("Website"),
            "_place_id": None,
        },
    )

    selected_ids = set(edited[edited["Include"]]["_place_id"].tolist())
    selected = [b for b in results if b.get("place_id") in selected_ids]

    with_website = [b for b in selected if b.get("website")]
    without_website = len(selected) - len(with_website)

    c1, c2, c3 = st.columns(3)
    c1.metric("Selected", len(selected))
    c2.metric("With website (can scrape)", len(with_website))
    c3.metric("No website", without_website)

    if without_website > 0:
        st.caption(
            f"ℹ️ {without_website} businesses have no website listed — "
            "we can't scrape emails from those but they'll still be saved."
        )

    st.divider()
    st.subheader("Save to search")
    save_col1, save_col2 = st.columns([1, 1])
    with save_col1:
        save_only = st.button(
            f"💾 Save {len(selected)} businesses (no scraping)",
            disabled=not selected,
        )
    with save_col2:
        save_and_run = st.button(
            f"🚀 Save + Run full pipeline ({len(selected)} businesses)",
            type="primary",
            disabled=not selected,
            help="Saves the search AND immediately starts a Volume-mode "
                 "scrape in the background: deep website crawl + Wayback + "
                 "NPI (for medical) + selective NeverBounce. Cost: ~$0.009/"
                 "biz, ~$1.80 per 200. When done, go to 🚀 Bulk Scrape or "
                 "📥 Export CSV to review the results.",
        )

    if save_only or save_and_run:
        search_id = storage.create_search(
            query=st.session_state.get("last_query", query),
            location=st.session_state.get("last_location", location),
            max_results=max_results,
        )
        count = storage.add_businesses_bulk(search_id, selected)

        if save_and_run:
            # Kick off the Volume-mode pipeline in the background.
            # Matches the default of the Bulk Scrape page: cheap
            # (~$0.009/biz), NPI on medical verticals, CMS-aware NB
            # interpretation, never picks generic inboxes. For hard
            # rows the user can opt into "Rescue empties" from Bulk
            # Scrape's UI on a later pass.
            try:
                from src import background_jobs
                from src.volume_mode import scrape_volume
                from src.volume_mode.pipeline import (
                    volume_result_to_scrape_result, reset_run_budget,
                )
                from src.lead_scoring import compute_lead_quality_score

                # Volume mode tracks a shared per-run cost cap — reset
                # so yesterday's spend doesn't gate today's job.
                reset_run_budget(25.0)

                # Pull fresh list with IDs
                pending = [b for b in storage.list_businesses(search_id=search_id)
                            if b.get("website")]

                def _worker(biz, job_id):
                    try:
                        vres = scrape_volume(biz, use_neverbounce=True)
                        result = volume_result_to_scrape_result(vres, biz)
                        storage.update_business_emails(biz["id"], result)
                        # Rescore using the updated row
                        fresh = storage.list_businesses(search_id=search_id)
                        updated = next((b for b in fresh if b["id"] == biz["id"]), None)
                        if updated:
                            s = compute_lead_quality_score(updated)
                            storage.update_lead_score(
                                biz["id"], s["score"], s["tier"],
                                all_emails=result.get("scraped_emails", []),
                            )
                        email = result.get("primary_email") or "(no email)"
                        return True, f"✓ {biz['business_name']} → {email}"
                    except Exception as e:
                        return False, f"❌ {biz['business_name']}: {type(e).__name__}: {e}"

                label = f"{query} in {location}" if location else query
                job_id = background_jobs.start(
                    job_type="bulk_volume_scrape",
                    items=pending,
                    worker_fn=_worker,
                    search_id=search_id,
                    max_workers=10,
                    metadata={"mode": "volume_oneclick", "search_label": label},
                )
                st.success(
                    f"🚀 Created search #{search_id} ({count} businesses). "
                    f"Volume-mode pipeline running in background "
                    f"(job `{job_id[:8]}`). Est. cost "
                    f"~${count * 0.009:.2f}. "
                    f"Head to **🚀 Bulk Scrape** to watch progress / run "
                    f"rescue passes, or **📥 Export CSV** when done."
                )
            except Exception as e:
                st.error(f"Search saved but pipeline failed to start: {e}")
        else:
            st.success(
                f"Created search #{search_id} with {count} businesses. "
                "Head to **🚀 Bulk Scrape** or **📧 Scrape Emails** next."
            )

        for key in ("last_results", "last_query", "last_location"):
            st.session_state.pop(key, None)
        st.rerun()

