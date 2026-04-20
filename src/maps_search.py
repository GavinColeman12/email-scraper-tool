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


# Businesses sometimes list a social-media page as their "website" on
# Google Maps. We can't scrape those for owner emails (they're walled
# gardens, not the business's own domain). Track them so the triangulation
# pipeline can skip the website-scrape agent cleanly instead of pulling
# irrelevant candidates off a social page.
_SOCIAL_ONLY_HOSTS = (
    "facebook.com", "fb.com", "instagram.com", "linkedin.com",
    "twitter.com", "x.com", "tiktok.com", "youtube.com",
    "yelp.com", "tripadvisor.com", "opentable.com", "doordash.com",
    "ubereats.com", "grubhub.com", "seamless.com", "pinterest.com",
    "wa.me", "wame.me",  # WhatsApp share links
)


def _is_real_business_website(url: str) -> bool:
    """Return False for social/review-only URLs that can't be scraped."""
    if not url:
        return False
    u = url.lower().strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "https://" + u  # tolerate bare domains
    for host in _SOCIAL_ONLY_HOSTS:
        if f"://{host}/" in u or f"://www.{host}/" in u or u.endswith(f"://{host}") or u.endswith(f"://www.{host}"):
            return False
    return True


def _parse_business(biz: dict) -> dict:
    """Normalize a SearchApi local_result into our format."""
    btype = biz.get("type") or biz.get("types") or ""
    if isinstance(btype, list):
        btype = btype[0] if btype else ""

    place_id = biz.get("place_id", "") or biz.get("data_id", "")
    maps_url = biz.get("link") or ""
    if not maps_url and place_id:
        maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

    raw_website = biz.get("website", "") or ""
    # Keep the raw value visible so operators can inspect, but flag
    # social-only URLs so the triangulation pipeline can bail out cleanly.
    website = raw_website if _is_real_business_website(raw_website) else ""

    return {
        "business_name": biz.get("title", "") or biz.get("name", ""),
        "address": biz.get("address", ""),
        "phone": biz.get("phone", ""),
        "website": website,
        "website_raw": raw_website,
        "website_is_social_only": bool(raw_website and not website),
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
    # Dental — 7 variants covers the full space
    "dental": ["dentist", "dental office", "dental clinic", "dental practice",
                "family dentistry", "cosmetic dentist", "general dentist"],
    "dental office": ["dentist", "dental clinic", "dental practice",
                       "family dentistry", "cosmetic dentist", "general dentist",
                       "emergency dentist"],
    "dentist": ["dental office", "dental clinic", "dental practice",
                 "family dentistry", "cosmetic dentist", "general dentist"],
    "dental clinic": ["dentist", "dental office", "dental practice",
                       "family dentistry", "cosmetic dentist", "general dentist"],
    "dental practice": ["dentist", "dental office", "dental clinic",
                         "family dentistry", "cosmetic dentist"],
    "dental care": ["dentist", "dental office", "dental clinic", "dental practice"],
    # Short-form shorthands that users commonly type
    "medical": ["medical clinic", "doctor", "physician", "urgent care", "family medicine"],
    "legal": ["law firm", "attorney", "lawyer", "law office"],
    "auto": ["auto repair", "mechanic", "auto shop", "car repair"],
    "fitness": ["gym", "fitness center", "fitness studio", "health club"],
    "beauty": ["salon", "hair salon", "beauty salon", "med spa"],
    "food": ["restaurant", "cafe", "diner", "eatery"],
    "plumbing": ["plumber", "plumbing service", "plumbing contractor"],
    "electrical": ["electrician", "electrical contractor", "electrical service"],
    "roofing": ["roofer", "roofing contractor", "roofing company"],
    "realty": ["real estate agent", "realtor", "real estate agency"],
    "accounting": ["accountant", "CPA", "accounting firm", "tax preparation"],
    "veterinary": ["veterinarian", "vet clinic", "animal hospital", "pet clinic"],
    "chiropractic": ["chiropractor", "chiropractic clinic", "chiropractic office"],
    "eye": ["optometrist", "eye doctor", "vision center", "optical shop"],
    "physical therapy": ["physical therapist", "PT clinic", "rehab clinic"],
    "insurance": ["insurance agent", "insurance broker", "insurance agency"],
    # Legal
    "law firm": ["attorney", "lawyer", "legal services", "law office",
                  "law practice", "legal counsel"],
    "attorney": ["law firm", "lawyer", "legal services", "law office"],
    "lawyer": ["law firm", "attorney", "legal services", "law office"],
    "law office": ["law firm", "attorney", "lawyer", "legal services"],
    # Food & beverage
    "restaurant": ["eatery", "dining", "bistro", "cafe", "grill", "kitchen"],
    "cafe": ["coffee shop", "coffeehouse", "bakery cafe", "espresso bar"],
    "coffee shop": ["cafe", "coffeehouse", "espresso bar"],
    # Medical
    "chiropractor": ["chiropractic clinic", "chiropractic office",
                      "spine care", "back pain clinic"],
    "physical therapy": ["physical therapist", "PT clinic", "rehab clinic",
                          "physiotherapy"],
    "dermatologist": ["dermatology clinic", "skin clinic", "skin doctor"],
    "veterinarian": ["vet clinic", "animal hospital", "pet clinic",
                      "veterinary hospital"],
    "orthodontist": ["orthodontics", "braces clinic", "invisalign provider"],
    "optometrist": ["eye doctor", "optical shop", "vision center",
                     "eye care center"],
    "pediatrician": ["pediatric clinic", "children's doctor", "kids doctor"],
    # Wellness / beauty
    "gym": ["fitness center", "fitness studio", "health club", "crossfit"],
    "salon": ["hair salon", "beauty salon", "styling studio"],
    "barber": ["barbershop", "men's haircuts", "barber shop"],
    "med spa": ["medspa", "medical spa", "aesthetic clinic", "botox clinic"],
    "spa": ["day spa", "wellness spa", "med spa"],
    "yoga studio": ["yoga", "hot yoga", "yoga classes"],
    # Home services
    "plumber": ["plumbing service", "plumbing company", "plumbing contractor",
                 "emergency plumber"],
    "electrician": ["electrical service", "electrical contractor",
                     "electrical company"],
    "hvac": ["hvac contractor", "heating and cooling", "air conditioning",
              "ac repair"],
    "roofer": ["roofing contractor", "roofing company", "roof repair"],
    "landscaper": ["landscaping company", "lawn care", "landscape design"],
    "pest control": ["exterminator", "bug control", "pest management"],
    "cleaning service": ["house cleaning", "maid service", "janitorial"],
    # Finance / professional
    "accountant": ["CPA", "accounting firm", "tax preparation", "tax advisor",
                    "bookkeeper"],
    "financial advisor": ["financial planner", "wealth manager", "investment advisor"],
    "insurance agent": ["insurance broker", "insurance agency"],
    # Real estate
    "real estate agent": ["realtor", "real estate agency", "realty office"],
    "realtor": ["real estate agent", "real estate agency"],
    # Retail
    "jewelry store": ["jeweler", "jewelry shop", "fine jewelry"],
    "florist": ["flower shop", "flower delivery", "florists"],
    # Auto
    "auto repair": ["mechanic", "auto shop", "car repair", "auto service"],
    "mechanic": ["auto repair", "auto shop", "car repair"],
}


def _normalize_query(q: str) -> str:
    """Lowercase + strip trailing 's' from the last word (dental clinics -> dental clinic)."""
    q = q.strip().lower()
    if q.endswith("s") and not q.endswith("ss"):
        # Only strip 's' if removing it leaves a known synonym key
        singular = q[:-1]
        if singular in QUERY_SYNONYMS:
            return singular
    return q


def _query_variants(query: str) -> list:
    """Return ordered list of queries to try, starting with the user's input."""
    q = _normalize_query(query)
    variants = [query]  # always try the exact user query first

    # Look up synonyms for normalized form, then for plural-stripped form
    synonyms = list(QUERY_SYNONYMS.get(q, []))
    if q.endswith("s") and q[:-1] in QUERY_SYNONYMS:
        synonyms.extend(QUERY_SYNONYMS[q[:-1]])

    variants.extend(synonyms)

    # Also try the singular form of the original query if it differs
    raw_lower = query.strip().lower()
    if raw_lower != q:
        variants.append(q)

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
