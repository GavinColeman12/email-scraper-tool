"""Email Scraper — find business emails from Google Maps + website scraping."""
import streamlit as st

from src import storage

st.set_page_config(
    page_title="Email Scraper",
    page_icon="📧",
    layout="wide",
)

storage.init_db()

st.title("📧 Email Scraper")
st.caption("Find direct business emails from Google Maps + website scraping. "
           "No Apollo needed — fresh emails that haven't been spammed.")

# ── Quick stats ──
searches = storage.list_searches()
total_businesses = sum(s.get("business_count", 0) for s in searches)
total_emails = sum(s.get("with_email", 0) for s in searches)

c1, c2, c3 = st.columns(3)
c1.metric("Total searches", len(searches))
c2.metric("Businesses found", total_businesses)
c3.metric("Emails scraped", total_emails)

st.divider()

# ── Workflow intro ──
st.markdown("""
### Workflow

1. **🔎 Find Businesses** — Search Google Maps for businesses by type + location (e.g. "dental clinics Manhattan")
2. **📧 Scrape Emails** — For each business, scrape their website for direct contact emails
3. **✅ Verify Deliverability** — Check MX records (free) + optional paid verification
4. **📥 Export CSV** — Download a CSV ready for import into the reputation audit tool

### Why this beats Apollo

- **Fresh emails** — scraped directly from business websites, not overused by every marketer
- **Direct-to-owner** — finds `dr.patel@` instead of `info@`
- **~$0.01 per email** vs Apollo's $0.10+ per contact
- **No subscription** — only pay for what you scrape
""")

st.divider()

# ── Recent searches ──
if searches:
    st.subheader("Recent searches")
    for s in searches[:10]:
        with st.container(border=True):
            cols = st.columns([4, 1, 1, 1])
            cols[0].markdown(
                f"**#{s['id']} — {s['query']}**  \n"
                f"_{s.get('location', '') or '(no location)'} · "
                f"{s.get('max_results', '?')} max results · "
                f"Created {s.get('created_at', '')[:10]}_"
            )
            cols[1].metric("Found", s.get("business_count", 0))
            cols[2].metric("With email", s.get("with_email", 0))
            if cols[3].button("🗑️ Delete", key=f"del_{s['id']}"):
                storage.delete_search(s["id"])
                st.rerun()
else:
    st.info("No searches yet. Head to **🔎 Find Businesses** to start.")
