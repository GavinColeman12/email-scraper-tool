"""
Decision-maker discovery.

Two techniques:
1. LinkedIn-via-Google search (SearchApi regular Google engine): query
   site:linkedin.com/in for "{business_name}" + role keywords, parse
   title/name from search result snippets. No LinkedIn scraping.
2. Website decision-maker extraction: stronger title regex, track
   person->email pairs when they co-occur in the same HTML block.
"""
import re
import requests

from src.secrets import get_secret

SEARCHAPI_URL = "https://www.searchapi.io/api/v1/search"

# Titles that indicate decision-making authority
DECISION_TITLES = [
    "Owner", "CEO", "Chief Executive", "Founder", "Co-Founder", "Cofounder",
    "President", "Managing Partner", "Managing Director", "Principal",
    "Partner", "Director", "Head of",
    # Professional services
    "Attorney", "Lawyer",
    # Medical / dental
    "Dr.", "Doctor", "DDS", "DMD", "MD",
    "Practice Owner", "Lead Dentist", "Lead Doctor", "Chief Dentist",
]

# Regex to pull "Name - Title at Company" from LinkedIn snippet format
# LinkedIn typically renders: "Jane Doe - Owner - City Dental | LinkedIn"
# or                          "Jane Doe - CEO at City Dental - LinkedIn"
LINKEDIN_SNIPPET_RE = re.compile(
    r"^([A-Z][a-zA-Z\-'.]+(?:\s+[A-Z][a-zA-Z\-'.]+){1,3})"  # Full name
    r"\s*[-–]\s*"                                           # separator
    r"([A-Z][^|\-–\n]*?)"                                   # Title
    r"(?:\s*(?:at|-|–|\|)\s*(.+?))?"                        # Optional company
    r"(?:\s*[-–|]\s*LinkedIn.*)?$",
    re.IGNORECASE,
)


def _looks_like_decision_title(title: str) -> bool:
    if not title:
        return False
    tl = title.lower()
    return any(dt.lower() in tl for dt in DECISION_TITLES)


def _linkedin_search(business_name: str, location: str = "") -> list:
    """
    Use Google via SearchApi to find LinkedIn profiles at this business.
    Returns a list of {name, title, source: 'linkedin'} dicts.
    """
    try:
        api_key = get_secret("SEARCHAPI_KEY")
    except Exception:
        return []
    if not api_key:
        return []

    # Query with role keywords to bias toward decision makers
    query_parts = [
        f'site:linkedin.com/in "{business_name}"',
        '("Owner" OR "CEO" OR "Founder" OR "President" OR "Principal" OR "Partner")',
    ]
    if location:
        query_parts.append(f'"{location}"')
    query = " ".join(query_parts)

    try:
        resp = requests.get(
            SEARCHAPI_URL,
            params={
                "engine": "google",
                "q": query,
                "api_key": api_key,
                "num": 10,
            },
            timeout=15,
        )
        data = resp.json()
    except Exception:
        return []

    people = []
    for r in (data.get("organic_results") or [])[:10]:
        # Title field is usually "Jane Doe - Owner - City Dental | LinkedIn"
        raw_title = r.get("title", "") or ""
        snippet = r.get("snippet", "") or ""

        m = LINKEDIN_SNIPPET_RE.match(raw_title.strip())
        if not m:
            continue

        name = m.group(1).strip()
        title = m.group(2).strip() if m.group(2) else ""
        company = m.group(3).strip() if m.group(3) else ""

        # Quality check — verify this is plausibly the right business
        bn_lower = business_name.lower()
        haystack = (raw_title + " " + snippet + " " + company).lower()
        name_tokens_in_business = sum(
            1 for tok in bn_lower.split() if len(tok) > 3 and tok in haystack
        )
        if name_tokens_in_business == 0:
            continue

        # Only keep decision-maker roles
        if not _looks_like_decision_title(title):
            continue

        parts = name.split()
        people.append({
            "name": name,
            "first": parts[0] if parts else "",
            "last": parts[-1] if len(parts) >= 2 else "",
            "title": title,
            "source": "linkedin",
        })

    # Dedupe by name
    seen = set()
    out = []
    for p in people:
        key = p["name"].lower()
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out[:3]


def find_decision_makers(business_name: str, location: str = "") -> list:
    """
    Find likely decision makers at a business using LinkedIn-via-Google.
    Returns list of {name, first, last, title, source} dicts, ranked best-first.
    Empty list if nothing found.
    """
    if not business_name:
        return []
    return _linkedin_search(business_name, location)
