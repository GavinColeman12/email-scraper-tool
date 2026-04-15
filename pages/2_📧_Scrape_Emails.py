import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import streamlit as st

from src import storage
from src.email_scraper import scrape_business_emails

st.set_page_config(page_title="Scrape Emails", page_icon="📧", layout="wide")
st.title("📧 Scrape Business Websites for Emails")

storage.init_db()

searches = storage.list_searches()
if not searches:
    st.warning("No searches yet. Go to **🔎 Find Businesses** first.")
    st.stop()

# ── Pick a search ──
labels = {s["id"]: f"#{s['id']} — {s['query']} ({s.get('with_email', 0)}/{s.get('business_count', 0)} scraped)"
          for s in searches}
search_id = st.selectbox("Search", options=list(labels.keys()),
                          format_func=lambda k: labels[k])

businesses = storage.list_businesses(search_id=search_id)
pending = [b for b in businesses if not b.get("primary_email") and b.get("website")]
already_scraped = [b for b in businesses if b.get("primary_email")]
no_website = [b for b in businesses if not b.get("website")]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total", len(businesses))
c2.metric("Pending scrape", len(pending))
c3.metric("Already scraped", len(already_scraped))
c4.metric("No website", len(no_website))

st.caption(
    "💡 Scraping is free — just web requests. Each business fetches ~8 pages "
    "(homepage + contact/about/team) and extracts emails via regex + mailto: links."
)

# ── Bulk scrape ──
if st.button(f"▶️ Scrape {len(pending)} pending businesses",
              disabled=not pending, type="primary"):
    progress = st.progress(0)
    status_box = st.empty()
    results_preview = st.empty()

    def _scrape_one(biz):
        result = scrape_business_emails(
            business_name=biz["business_name"],
            website=biz.get("website", ""),
        )
        storage.update_business_emails(biz["id"], result)
        return biz, result

    completed = 0
    scraped_so_far = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_scrape_one, b) for b in pending]
        for fut in as_completed(futures):
            try:
                biz, result = fut.result()
            except Exception as e:
                biz, result = {"business_name": "?"}, {"primary_email": "", "scraped_emails": []}
                st.write(f"⚠️ Error on {biz.get('business_name')}: {e}")
            completed += 1
            progress.progress(completed / len(pending))
            status_box.write(
                f"**{completed}/{len(pending)}** · {biz.get('business_name')} → "
                f"{result.get('primary_email') or '(no email found)'}"
            )
            if result.get("primary_email"):
                scraped_so_far.append({
                    "Name": biz.get("business_name"),
                    "Email": result["primary_email"],
                    "Scraped?": "✅" if result.get("scraped_emails") else "🤔 pattern-based",
                })
                if len(scraped_so_far) % 5 == 0:
                    results_preview.dataframe(
                        pd.DataFrame(scraped_so_far),
                        use_container_width=True, hide_index=True,
                    )

    progress.empty()
    status_box.success(f"✅ Done — scraped {completed} businesses")
    st.rerun()

# ── Results table ──
st.divider()
st.subheader("All businesses in this search")

filter_option = st.radio("Show",
                         ["all", "with_email", "scraped_found", "pattern_only",
                          "no_email", "no_website"],
                         horizontal=True,
                         format_func=lambda k: {
                             "all": "All",
                             "with_email": "With email",
                             "scraped_found": "Scraped (found on site)",
                             "pattern_only": "Pattern-based only",
                             "no_email": "No email",
                             "no_website": "No website",
                         }[k])


def matches(b):
    has_email = bool(b.get("primary_email"))
    has_site = bool(b.get("website"))
    scraped = bool(b.get("scraped_emails"))
    if filter_option == "with_email":
        return has_email
    if filter_option == "scraped_found":
        return scraped
    if filter_option == "pattern_only":
        return has_email and not scraped
    if filter_option == "no_email":
        return has_site and not has_email
    if filter_option == "no_website":
        return not has_site
    return True


filtered = [b for b in businesses if matches(b)]

rows = []
for b in filtered:
    scraped_emails = b.get("scraped_emails") or []
    constructed = b.get("constructed_emails") or []
    source = "—"
    if b.get("primary_email"):
        source = "🎯 scraped" if b["primary_email"] in scraped_emails else "🤔 pattern"
    rows.append({
        "id": b["id"],
        "Name": b["business_name"],
        "Email": b.get("primary_email") or "",
        "Source": source,
        "Contact": b.get("contact_name") or "—",
        "All scraped": ", ".join(scraped_emails[:3]) if scraped_emails else "—",
        "Website": b.get("website") or "—",
        "Status": b.get("email_status") or "not verified",
    })

if rows:
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True,
                 column_config={"id": None, "Website": st.column_config.LinkColumn()})
else:
    st.info("No businesses match the current filter.")

# ── Manual override ──
with st.expander("✏️ Override email for a specific business"):
    names = {b["id"]: b["business_name"] for b in filtered}
    if names:
        pick = st.selectbox("Business", options=list(names.keys()),
                             format_func=lambda k: names[k], key="ov_pick")
        chosen = next((b for b in filtered if b["id"] == pick), None)
        if chosen:
            options = [""]
            if chosen.get("scraped_emails"):
                options.extend(chosen["scraped_emails"])
            if chosen.get("constructed_emails"):
                options.extend(chosen["constructed_emails"])
            options.append("Custom...")
            current = chosen.get("primary_email", "")
            selected = st.selectbox(
                "Pick an email",
                options=options,
                index=options.index(current) if current in options else 0,
                key="ov_email",
            )
            custom = ""
            if selected == "Custom...":
                custom = st.text_input("Custom email", key="ov_custom")
            if st.button("💾 Save override"):
                new_email = custom if selected == "Custom..." else selected
                storage.override_primary_email(pick, new_email)
                st.success(f"Updated to {new_email or '(blank)'}")
                st.rerun()
