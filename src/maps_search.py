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


def search_businesses(query: str, location: str = "",
                      max_results: int = 50) -> list:
    """
    Search Google Maps for businesses matching the query in the location.
    Paginates automatically. Returns up to max_results businesses.

    Typical query forms:
      search_businesses("dental clinic", "Manhattan NYC")
      search_businesses("law firm", "Brooklyn NY", max_results=100)
    """
    full_query = f"{query} {location}".strip()
    results = []
    start = 0

    while len(results) < max_results:
        params = {
            "engine": "google_maps",
            "q": full_query,
        }
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
                    results.append(_parse_business(pr))
            break

        for biz in local_results:
            parsed = _parse_business(biz)
            # Dedupe by place_id
            if parsed.get("place_id") and any(
                r.get("place_id") == parsed["place_id"] for r in results
            ):
                continue
            results.append(parsed)
            if len(results) >= max_results:
                break

        # SearchApi Google Maps paginates in increments of 20
        start += 20
        if start >= 100:  # Practical cap per query
            break
        time.sleep(0.3)

    return results[:max_results]


def estimate_cost(max_results: int) -> float:
    """Estimate SearchApi credit cost for a given search size."""
    pages = max(1, (max_results + 19) // 20)
    # SearchApi charges ~1 credit per call, typically ~$0.005 per credit
    return pages * 0.005
