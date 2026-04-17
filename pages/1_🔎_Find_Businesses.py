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

    est = estimate_cost(max_results)
    st.caption(
        f"💰 Estimated cost: ~${est:.2f} SearchApi credits "
        f"({(max_results + 19) // 20} API calls)"
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
            help="Saves the search AND immediately starts the Verified scraping job in "
                 "the background: scrape emails, Haiku extraction, SMTP verification, "
                 "lead scoring. When done, go to 📥 Export CSV to review the results.",
        )

    if save_only or save_and_run:
        search_id = storage.create_search(
            query=st.session_state.get("last_query", query),
            location=st.session_state.get("last_location", location),
            max_results=max_results,
        )
        count = storage.add_businesses_bulk(search_id, selected)

        if save_and_run:
            # Kick off the Verified pipeline in the background
            try:
                from src import background_jobs
                from src.deep_scraper import deep_scrape_business_emails
                from src.lead_scoring import compute_lead_quality_score

                # Pull fresh list with IDs
                pending = [b for b in storage.list_businesses(search_id=search_id)
                            if b.get("website")]

                def _worker(biz, job_id):
                    try:
                        addr = biz.get("address", "") or biz.get("location", "")
                        city = addr.split(",")[0].strip() if addr else ""
                        result = deep_scrape_business_emails(
                            business_name=biz["business_name"],
                            website=biz.get("website", ""),
                            location=city,
                            verify_with_mx=True,
                        )
                        storage.update_business_emails(biz["id"], result)
                        # Rescore
                        fresh = storage.list_businesses(search_id=search_id)
                        updated = next((b for b in fresh if b["id"] == biz["id"]), None)
                        if updated:
                            s = compute_lead_quality_score(updated)
                            storage.update_lead_score(
                                biz["id"], s["score"], s["tier"],
                                all_emails=result.get("scraped_emails", []),
                            )
                        return True, f"✓ {biz['business_name']} → {result.get('primary_email') or '(skip)'}"
                    except Exception as e:
                        return False, f"❌ {biz['business_name']}: {type(e).__name__}"

                job_id = background_jobs.start(
                    job_type="bulk_deep_scrape",
                    items=pending,
                    worker_fn=_worker,
                    search_id=search_id,
                    max_workers=6,
                    metadata={"mode": "verified_oneclick",
                              "search_label": labels_for_search(query, location)},
                )
                st.success(
                    f"🚀 Created search #{search_id} ({count} businesses). "
                    f"Full pipeline running in background (job `{job_id[:8]}`). "
                    f"Head to **📥 Export CSV** when it's done to review results."
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


def labels_for_search(q, loc):
    return f"{q} in {loc}" if loc else q
