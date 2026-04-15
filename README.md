# Email Scraper

A standalone Streamlit app that finds direct business emails by combining Google Maps search with website scraping and MX-record verification. Exports Apollo-compatible CSVs for import into outreach tools.

Designed as a cheaper, fresher alternative to Apollo.io — the emails you scrape here haven't been sold & spammed by every B2B tool on the market.

## Workflow

1. **🔎 Find Businesses** — Search Google Maps (via SearchApi.io) for "dental clinic in Manhattan NYC", etc. Preview and save selected businesses to a search.
2. **📧 Scrape Emails** — For each business with a website, fetch homepage + `/contact`, `/about`, `/team`, `/doctors` etc. Extract `mailto:` links + regex-match emails. Construct patterns (`firstname@domain`, `info@domain`) when nothing is found.
3. **✅ Verify Deliverability** — Three modes:
   - **Free (MX-only)**: DNS MX lookup — catches ~80% of dead emails, $0
   - **Hybrid**: MX first, then ZeroBounce only for MX-passing emails (~$0.005/email)
   - **Paid only**: ZeroBounce for every email (~$0.007/email, ~95% accuracy)
4. **📥 Export CSV** — Download as generic CSV or Apollo-format CSV for import into reputation-audit-tool (or any outreach tool).

## Setup

```bash
python3 -m pip install -r requirements.txt
cp .env.example .env
# Add SEARCHAPI_KEY (required) and ZEROBOUNCE_API_KEY (optional)
streamlit run app.py
```

## Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `SEARCHAPI_KEY` | Yes | Google Maps search (SearchApi.io) |
| `ZEROBOUNCE_API_KEY` | No | Paid email deliverability check |

## Data

- SQLite at `data/scraper.db` — searches + businesses + scraped emails + verification status
- Persists across sessions locally. For Streamlit Cloud deployment, migrate to a hosted Postgres (planned).

## Deploying to Streamlit Cloud

1. Push to GitHub
2. New app → point to `app.py`
3. Add `SEARCHAPI_KEY` (and optionally `ZEROBOUNCE_API_KEY`) to the app's secrets
4. Note: Streamlit Cloud storage is ephemeral — the SQLite DB resets on redeploy. Export CSVs regularly.

## Costs

- **SearchApi**: ~$0.005 per 20 results (one API call returns ~20 businesses)
- **Website scraping**: free (just HTTP requests, ~8 pages per business)
- **MX verification**: free (DNS lookups)
- **ZeroBounce** (optional): $0.007/email, ~95% accuracy
