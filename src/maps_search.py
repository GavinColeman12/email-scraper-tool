"""
Paginated Google Maps business search via SearchApi.io.
Returns up to `max_results` businesses with name, address, website, phone, place_id.
"""
import time
import requests

from src.secrets import get_secret

BASE_URL = "https://www.searchapi.io/api/v1/search"


class SearchError(Exception):
    pass


def _call(params: dict) -> dict:
    api_key = get_secret("SEARCHAPI_KEY")
    if not api_key:
        raise SearchError("SEARCHAPI_KEY not set. Add it to .env or Streamlit Cloud secrets.")

    call_params = dict(params)
    call_params["api_key"] = api_key

    try:
        resp = requests.get(BASE_URL, params=call_params, timeout=30)
    except requests.RequestException as e:
        raise SearchError(f"Network error: {e}") from e

    if resp.status_code == 401 or resp.status_code == 403:
        raise SearchError("SearchApi authentication failed — check SEARCHAPI_KEY.")
    if resp.status_code == 429:
        raise SearchError("SearchApi quota exceeded. Upgrade your plan or wait.")

    try:
        data = resp.json()
    except Exception as e:
        raise SearchError(f"Invalid JSON from SearchApi: {e}") from e

    if isinstance(data, dict) and data.get("error"):
        raise SearchError(f"SearchApi error: {data['error']}")

    return data


def _parse_business(biz: dict) -> dict:
    """Normalize a SearchApi local_result into our format."""
    btype = biz.get("type") or biz.get("types") or ""
    if isinstance(btype, list):
        btype = btype[0] if btype else ""

    place_id = biz.get("place_id", "") or biz.get("data_id", "")
    maps_url = biz.get("link") or ""
    if not maps_url and place_id:
        maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

    return {
        "business_name": biz.get("title", "") or biz.get("name", ""),
        "address": biz.get("address", ""),
        "phone": biz.get("phone", ""),
        "website": biz.get("website", ""),
        "rating": float(biz.get("rating") or 0),
        "review_count": int(biz.get("reviews") or biz.get("reviews_count") or 0),
        "place_id": place_id,
        "google_maps_url": maps_url,
        "business_type": btype,
    }


# When Google Maps caps a single query at 20-60 results, these synonyms
# let us keep going by re-querying with related terms. Picked to be
# semantically equivalent — all return the same kind of business.
QUERY_SYNONYMS = {
    "dental office": ["dentist", "dental clinic", "dental practice", "family dentistry", "cosmetic dentist"],
    "dentist": ["dental office", "dental clinic", "dental practice", "family dentistry"],
    "dental clinic": ["dentist", "dental office", "dental practice"],
    "law firm": ["attorney", "lawyer", "legal services", "law office"],
    "attorney": ["law firm", "lawyer", "legal services"],
    "lawyer": ["law firm", "attorney", "legal services"],
    "restaurant": ["eatery", "dining", "bistro", "cafe"],
    "chiropractor": ["chiropractic clinic", "chiropractic office", "spine care"],
    "accountant": ["CPA", "accounting firm", "tax preparation"],
    "veterinarian": ["vet clinic", "animal hospital", "pet clinic"],
    "plumber": ["plumbing service", "plumbing company", "plumbing contractor"],
    "electrician": ["electrical service", "electrical contractor"],
    "hvac": ["hvac contractor", "heating and cooling", "air conditioning"],
    "roofer": ["roofing contractor", "roofing company"],
    "gym": ["fitness center", "fitness studio", "health club"],
    "salon": ["hair salon", "beauty salon"],
    "barber": ["barbershop", "men's haircuts"],
    "med spa": ["medspa", "medical spa", "aesthetic clinic"],
    "orthodontist": ["orthodontics", "braces clinic"],
    "optometrist": ["eye doctor", "optical shop", "vision center"],
}


def _query_variants(query: str) -> list:
    """Return ordered list of queries to try, starting with the user's input."""
    q = query.strip().lower()
    variants = [query]  # always try the exact user query first
    for synonym in QUERY_SYNONYMS.get(q, []):
        variants.append(synonym)
    # Dedupe case-insensitively, preserving order
    seen = set()
    out = []
    for v in variants:
        k = v.strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(v)
    return out


def _single_query_paginated(full_query: str, max_results: int,
                             seen_place_ids: set) -> list:
    """Run one query with full pagination, deduped against seen_place_ids."""
    results = []
    start = 0
    empty_pages = 0

    while len(results) < max_results:
        params = {"engine": "google_maps", "q": full_query}
        if start > 0:
            params["start"] = start

        try:
            data = _call(params)
        except SearchError:
            raise

        local_results = data.get("local_results", []) or []
        if not local_results:
            # Edge case: single-match shapes
            for key in ("place_result", "place_results", "knowledge_graph"):
                pr = data.get(key)
                if pr and isinstance(pr, dict) and (pr.get("title") or pr.get("name")):
                    parsed = _parse_business(pr)
                    pid = parsed.get("place_id")
                    if pid and pid not in seen_place_ids:
                        seen_place_ids.add(pid)
                        results.append(parsed)
            empty_pages += 1
            if empty_pages >= 2:  # two empty pages in a row = genuine end
                break
            start += 20
            time.sleep(0.3)
            continue

        empty_pages = 0
        new_this_page = 0
        for biz in local_results:
            parsed = _parse_business(biz)
            pid = parsed.get("place_id")
            if pid and pid in seen_place_ids:
                continue
            if pid:
                seen_place_ids.add(pid)
            results.append(parsed)
            new_this_page += 1
            if len(results) >= max_results:
                break

        # If the page produced nothing new (all dupes), Google is recycling —
        # further pagination won't help on this query.
        if new_this_page == 0:
            break

        start += 20
        # Softer upper bound — try up to 10 pages per query (200 offset)
        # before giving up. Google Maps rarely goes deeper than this.
        if start >= 200:
            break
        time.sleep(0.3)

    return results


def search_businesses(query: str, location: str = "",
                      max_results: int = 50) -> list:
    """
    Search Google Maps for businesses matching the query in the location.

    Strategy:
    1. Run the exact user query with full pagination
    2. If still below max_results, retry with synonym variants (e.g.
       "dental office" → "dentist" → "dental clinic")
    3. Dedupe everything by place_id across all variants

    Typical query forms:
      search_businesses("dental clinic", "Manhattan NYC")
      search_businesses("law firm", "Brooklyn NY", max_results=100)
    """
    all_results = []
    seen_place_ids = set()

    for variant in _query_variants(query):
        needed = max_results - len(all_results)
        if needed <= 0:
            break
        full_query = f"{variant} {location}".strip()
        try:
            batch = _single_query_paginated(full_query, needed, seen_place_ids)
        except SearchError:
            # Propagate auth/quota errors; they won't be fixed by more queries
            if not all_results:
                raise
            break
        all_results.extend(batch)
        if len(all_results) >= max_results:
            break

    return all_results[:max_results]


def estimate_cost(max_results: int) -> float:
    """Estimate SearchApi credit cost for a given search size.

    Upper bound — the scraper may use synonym variants if the base query
    doesn't return enough results, adding ~1 extra API call per variant.
    """
    pages = max(1, (max_results + 19) // 20)
    # Add ~2 extra calls to budget for synonym variants on larger searches
    if max_results > 40:
        pages += 2
    # SearchApi charges ~1 credit per call, typically ~$0.005 per credit
    return pages * 0.005
