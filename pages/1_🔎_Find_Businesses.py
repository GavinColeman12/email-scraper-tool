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
    max_results = c3.slider("Max results", 10, 100, 40)

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
            st.success(f"Found **{len(results)}** businesses.")
            st.session_state["last_results"] = results
            st.session_state["last_query"] = query
            st.session_state["last_location"] = location

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
    if st.button(f"💾 Save {len(selected)} businesses to new search",
                  type="primary", disabled=not selected):
        search_id = storage.create_search(
            query=st.session_state.get("last_query", query),
            location=st.session_state.get("last_location", location),
            max_results=max_results,
        )
        count = storage.add_businesses_bulk(search_id, selected)
        st.success(
            f"Created search #{search_id} with {count} businesses. "
            "Head to **📧 Scrape Emails** next."
        )
        # Clear the preview
        for key in ("last_results", "last_query", "last_location"):
            st.session_state.pop(key, None)
        st.rerun()
