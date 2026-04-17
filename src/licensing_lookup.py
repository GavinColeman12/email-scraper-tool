"""
Licensing-board lookups for regulated-profession verticals.

Uses public registries to return authoritative provider names for a
business. These names flow into our people list with higher authority
than team-page extraction, which anchors email construction with
stronger confidence.

V1 scope: NPI Registry (medical) + ADA Find-A-Dentist (dental).
Future: state bar associations (lawyers), state RE commissions.
"""
import re
import sys
import time

import requests

NPI_API = "https://npiregistry.cms.hhs.gov/api/"
ADA_API = "https://findadentist.ada.org/api/v1/dentists/search"
HTTP_TIMEOUT = 10
_RATE_LIMIT_DELAY = 0.5  # s between external calls
_LAST_CALL = [0.0]

_VERTICAL_KEYS_MEDICAL = {
    "medical", "medical_clinic", "medical practice", "clinic",
    "chiropractor", "chiropractic", "physical_therapy", "pt",
    "physiotherapy", "doctor", "physician",
}
_VERTICAL_KEYS_DENTAL = {
    "dental", "dental_practice", "dental_clinic", "dentist",
    "dental_office", "orthodontist",
}


def _rate_limit():
    elapsed = time.time() - _LAST_CALL[0]
    if elapsed < _RATE_LIMIT_DELAY:
        time.sleep(_RATE_LIMIT_DELAY - elapsed)
    _LAST_CALL[0] = time.time()


# ── NPI Registry (medical providers) ─────────────────────────────────

def lookup_medical_providers(business_name: str, city: str = "",
                              state: str = "",
                              street_address: str = "",
                              taxonomy: str = "",
                              postal_code: str = "") -> list:
    """
    Query the NPI Registry for providers practicing at this location.

    Strategy:
      1. Query NPI-1 (individuals) by postal_code (if available) OR city+state,
         filtered by taxonomy
      2. Filter results to those whose practice address matches the business's
         street_address OR contains distinctive business-name tokens

    Returns: [{name, first, last, title, source, authority, is_decision_maker}]
    """
    if not state and not postal_code:
        return []

    _rate_limit()

    params = {
        "version": "2.1",
        "enumeration_type": "NPI-1",
        "state": state.strip().upper()[:2] if state else "",
        "limit": 200,
    }
    # Postal code is MUCH more narrow than city — use when available
    if postal_code:
        params["postal_code"] = postal_code.strip()[:5]
    elif city:
        params["city"] = city.strip()[:50]
    if taxonomy:
        params["taxonomy_description"] = taxonomy

    try:
        resp = requests.get(NPI_API, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception as e:
        print(f"[licensing_lookup] NPI error: {e}", file=sys.stderr)
        return []

    # Normalize the matching targets
    biz_lower = (business_name or "").lower()
    addr_lower = (street_address or "").lower()
    # Pick a distinctive substring of the street address (e.g. "125 main st")
    addr_tokens = re.findall(r"\b(\d+\s+\w+(?:\s+\w+)?)", addr_lower)
    biz_tokens = {t for t in re.findall(r"[a-z]{4,}", biz_lower)
                   if t not in ("dental", "medical", "clinic", "practice",
                                 "office", "group", "associates", "care",
                                 "center", "family", "health")}

    providers = []
    seen_names = set()

    for result in data.get("results", [])[:50]:
        basic = result.get("basic", {}) or {}
        first = (basic.get("first_name") or "").strip().title()
        last = (basic.get("last_name") or "").strip().title()
        credential = (basic.get("credential") or "").strip()
        if not first or not last:
            continue

        # Find the LOCATION address (not MAILING)
        location_addrs = [
            a for a in (result.get("addresses") or [])
            if a.get("address_purpose") == "LOCATION"
        ]
        if not location_addrs:
            continue

        matched = False
        for addr in location_addrs:
            provider_addr = (addr.get("address_1") or "").lower()
            provider_org = ""
            # Basic may not have org; check taxonomies for practice context
            # Address match
            if addr_tokens and any(tok in provider_addr for tok in addr_tokens):
                matched = True
                break
            # Also allow match by business-name tokens appearing in address
            # (some practices embed the name in the street address or suite)
            if biz_tokens and any(tok in provider_addr for tok in biz_tokens):
                matched = True
                break

        if not matched:
            continue

        full = f"Dr. {first} {last}"
        if credential:
            full += f", {credential}"
        key = full.lower()
        if key in seen_names:
            continue
        seen_names.add(key)
        providers.append({
            "name": full,
            "first": first,
            "last": last,
            "title": credential or "Medical Provider (NPI-registered)",
            "source": "npi_registry",
            "authority": 15,
            "is_decision_maker": True,
        })

    return providers[:10]


# ── ADA Find-A-Dentist ───────────────────────────────────────────────

def lookup_dentists(business_name: str, city: str = "",
                    state: str = "",
                    street_address: str = "",
                    postal_code: str = "") -> list:
    """
    Query NPI Registry for dentists at this practice's location.
    NPI includes all licensed dentists (taxonomy 'Dentist').

    Uses the same provider-search logic as medical but filtered to
    dental taxonomy.
    """
    return lookup_medical_providers(
        business_name=business_name,
        city=city,
        state=state,
        street_address=street_address,
        postal_code=postal_code,
        taxonomy="Dentist",
    )


# ── Dispatcher ───────────────────────────────────────────────────────

def lookup_licensed_providers(vertical: str, business_name: str,
                               city: str = "", state: str = "",
                               street_address: str = "",
                               postal_code: str = "") -> list:
    """
    Dispatch to the right lookup based on vertical. Returns [] if the
    vertical isn't regulated or no providers found.
    """
    if not business_name:
        return []

    v = (vertical or "").lower().strip().replace("-", "_").replace(" ", "_")

    try:
        if v in _VERTICAL_KEYS_MEDICAL or any(k in v for k in ("medical", "doctor", "chiro")):
            return lookup_medical_providers(
                business_name, city, state, street_address, postal_code=postal_code)
        if v in _VERTICAL_KEYS_DENTAL or "dental" in v or "dentist" in v:
            return lookup_dentists(
                business_name, city, state, street_address, postal_code=postal_code)
    except Exception as e:
        print(f"[licensing_lookup] dispatch error: {e}", file=sys.stderr)
        return []

    return []


def parse_location(address: str) -> tuple:
    """Extract (city, state, postal_code, street) from a full address string."""
    if not address:
        return ("", "", "", "")
    # Simple heuristic: 'Street, City, ST 12345'
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 2:
        street = parts[0] if len(parts) >= 3 else ""
        city = parts[-2] if len(parts) >= 3 else parts[0]
        state_zip = parts[-1]
        state_match = re.search(r"\b([A-Z]{2})\b", state_zip)
        state = state_match.group(1) if state_match else ""
        zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", state_zip)
        postal = zip_match.group(1) if zip_match else ""
        return (city, state, postal, street)
    return ("", "", "", "")
