"""
Research agents for deep decision-maker discovery.

Each agent is a standalone function that takes a business context
(name, website, domain, location) and returns structured findings.
They run in parallel, and their outputs are fed to a Claude-powered
synthesizer that picks the best contact.

All agents are designed to fail gracefully — a missing source shouldn't
break the pipeline. Return empty lists/dicts on error.
"""
import json
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.secrets import get_secret

SEARCHAPI_URL = "https://www.searchapi.io/api/v1/search"
TIMEOUT = 12

DECISION_TITLES_RE = re.compile(
    r"(CEO|Chief Executive Officer|Founder|Co-?Founder|Owner|President|"
    r"Managing Partner|Managing Director|Principal|Partner|Director|"
    r"Chief \w+ Officer|Lead Dentist|Practice Owner)",
    re.IGNORECASE,
)


# ──────────────────────────────────────────────────────────────────
# Agent 1: Website team/about page extraction (richer than base scraper)
# ──────────────────────────────────────────────────────────────────

def website_people_agent(website: str, fetched_pages: dict) -> dict:
    """
    Parse already-fetched team/about pages for person cards.
    Looks for name + title pairs in structured HTML (h3 + p, cards, etc.)

    `fetched_pages`: dict of {url_path: html} already downloaded by the
    base scraper — we avoid re-fetching.
    """
    people = []
    for path, html in (fetched_pages or {}).items():
        if not html:
            continue
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            continue

        # Strategy A: look for name elements (h2-h4) with a title sibling
        for tag in soup.find_all(["h2", "h3", "h4"]):
            name = tag.get_text(" ", strip=True)
            if not name or len(name.split()) < 2 or len(name) > 80:
                continue
            # Must look like a person name (capitalized, no all-caps)
            if not re.match(r"^[A-Z][a-z]+\s+[A-Z][a-zA-Z'\-.]+", name):
                continue
            # Look for title in next sibling or parent's next sibling
            title_text = ""
            for sibling in [tag.find_next_sibling(),
                             tag.parent.find_next_sibling() if tag.parent else None]:
                if sibling:
                    t = sibling.get_text(" ", strip=True)[:120]
                    if t and DECISION_TITLES_RE.search(t):
                        title_text = t
                        break
            parts = name.split()
            people.append({
                "name": name,
                "first": parts[0],
                "last": parts[-1] if len(parts) >= 2 else "",
                "title": title_text,
                "source": f"website:{path or 'home'}",
                "is_decision_maker": bool(DECISION_TITLES_RE.search(title_text)),
            })

    # Dedupe by name
    seen = set()
    out = []
    for p in people:
        key = p["name"].lower()
        if key not in seen:
            seen.add(key)
            out.append(p)
    return {"people": out[:10]}


# ──────────────────────────────────────────────────────────────────
# Agent 2: Schema.org JSON-LD Person/Organization extraction
# ──────────────────────────────────────────────────────────────────

def schema_org_agent(fetched_pages: dict) -> dict:
    """
    Parse JSON-LD structured data for Person + Organization.founder/employee.
    Many modern sites (especially ones built on Squarespace, Wix, Shopify)
    embed this automatically. When present, it's extremely reliable.
    """
    found = []
    for path, html in (fetched_pages or {}).items():
        if not html:
            continue
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            continue

        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.text or ""
            if not raw.strip():
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            # Normalize to a list
            items = data if isinstance(data, list) else [data]
            for item in items:
                _extract_schema_persons(item, found, source_path=path)

    # Dedupe by name
    seen = set()
    out = []
    for p in found:
        key = p["name"].lower()
        if key not in seen:
            seen.add(key)
            out.append(p)
    return {"people": out[:10]}


def _extract_schema_persons(node, out_list, source_path=""):
    """Walk a JSON-LD structure and extract Person entities."""
    if not isinstance(node, dict):
        return
    node_type = node.get("@type", "")
    if isinstance(node_type, list):
        node_type = " ".join(node_type)

    if "Person" in str(node_type):
        name = node.get("name", "")
        if name and isinstance(name, str):
            parts = name.split()
            out_list.append({
                "name": name,
                "first": parts[0] if parts else "",
                "last": parts[-1] if len(parts) >= 2 else "",
                "title": node.get("jobTitle", "") or node.get("title", ""),
                "email": (node.get("email", "") or "").replace("mailto:", ""),
                "source": f"schema.org:{source_path or 'home'}",
                "is_decision_maker": bool(DECISION_TITLES_RE.search(
                    node.get("jobTitle", "") or "")),
            })

    # Recurse into founder, employee, member, leader
    for field in ("founder", "founders", "employee", "employees",
                   "member", "members", "leader", "director"):
        val = node.get(field)
        if isinstance(val, list):
            for v in val:
                _extract_schema_persons(v, out_list, source_path)
        elif isinstance(val, dict):
            _extract_schema_persons(val, out_list, source_path)


# ──────────────────────────────────────────────────────────────────
# Agent 3: LinkedIn via Google (existing, wrapped)
# ──────────────────────────────────────────────────────────────────

def linkedin_agent(business_name: str, location: str = "") -> dict:
    try:
        from src.people_finder import find_decision_makers
        people = find_decision_makers(business_name, location) or []
    except Exception:
        people = []
    return {"people": [
        {**p, "is_decision_maker": True, "source": "linkedin"}
        for p in people
    ]}


# ──────────────────────────────────────────────────────────────────
# Agent 4: Press/news search via Google
# ──────────────────────────────────────────────────────────────────

def press_agent(business_name: str, location: str = "") -> dict:
    """
    Search Google for press articles mentioning decision makers at this
    business. Example queries:
      "CEO of {business}"
      "founder {business}"
      "{business} announces"
    Parses the snippets for "Name, [Title] of Business" patterns.
    """
    try:
        api_key = get_secret("SEARCHAPI_KEY")
    except Exception:
        return {"people": []}
    if not api_key:
        return {"people": []}

    queries = [
        f'"{business_name}" (CEO OR founder OR owner OR president)',
    ]
    if location:
        queries.append(f'"{business_name}" "{location}" (founder OR owner)')

    found = []
    for q in queries:
        try:
            resp = requests.get(
                SEARCHAPI_URL,
                params={"engine": "google", "q": q, "api_key": api_key, "num": 10},
                timeout=TIMEOUT,
            )
            data = resp.json()
        except Exception:
            continue

        for result in (data.get("organic_results") or [])[:10]:
            title = (result.get("title", "") or "") + " "
            snippet = (result.get("snippet", "") or "")
            text = title + snippet

            # Skip LinkedIn (handled by linkedin_agent)
            if "linkedin.com/in" in (result.get("link", "") or ""):
                continue

            # Pattern: "Name, [Role] of/at [Business]"
            # Or:      "[Role] [Name] of [Business]"
            for m in re.finditer(
                r"([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z'\-.]+){1,2})"
                r"\s*,\s*"
                r"((?:CEO|Chief \w+|Founder|Co-?Founder|Owner|President|"
                r"Managing Partner|Principal|Director)[^,\n.]{0,60})",
                text,
            ):
                name = m.group(1).strip()
                title = m.group(2).strip()
                # Sanity: business name must appear nearby
                if business_name.lower()[:15] not in text.lower():
                    continue
                parts = name.split()
                found.append({
                    "name": name,
                    "first": parts[0] if parts else "",
                    "last": parts[-1] if len(parts) >= 2 else "",
                    "title": title,
                    "source": "press",
                    "is_decision_maker": True,
                    "snippet": snippet[:200],
                })

            # Pattern: "Role Name" at start
            m = re.match(
                r"^(CEO|Founder|Co-?Founder|Owner|President|Managing Partner)\s+"
                r"([A-Z][a-z]+\s+[A-Z][a-zA-Z'\-.]+)",
                text.strip(),
            )
            if m:
                role = m.group(1).strip()
                name = m.group(2).strip()
                if business_name.lower()[:15] in text.lower():
                    parts = name.split()
                    found.append({
                        "name": name,
                        "first": parts[0] if parts else "",
                        "last": parts[-1] if len(parts) >= 2 else "",
                        "title": role,
                        "source": "press",
                        "is_decision_maker": True,
                        "snippet": snippet[:200],
                    })

    # Dedupe
    seen = set()
    out = []
    for p in found:
        key = p["name"].lower()
        if key not in seen:
            seen.add(key)
            out.append(p)
    return {"people": out[:5]}


# ──────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────

def run_all_agents(business_name: str, website: str, domain: str,
                   location: str = "",
                   fetched_pages: dict = None) -> dict:
    """
    Run all research agents in parallel. Returns a dict of findings:
      {
        "website": {"people": [...]},
        "schema": {"people": [...]},
        "linkedin": {"people": [...]},
        "press": {"people": [...]},
      }
    """
    results = {}
    tasks = {
        "website": (website_people_agent, (website, fetched_pages or {})),
        "schema": (schema_org_agent, (fetched_pages or {},)),
        "linkedin": (linkedin_agent, (business_name, location)),
        "press": (press_agent, (business_name, location)),
    }

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {
            ex.submit(fn, *args): name
            for name, (fn, args) in tasks.items()
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                results[name] = {"people": [], "error": str(e)}

    return results
