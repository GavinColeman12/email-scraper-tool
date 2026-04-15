"""
Scrape emails from a business website.
Strategies:
  1. Fetch homepage + common contact pages (/contact, /about, /team, etc.)
  2. Extract all mailto: links (highest signal)
  3. Regex sweep for email patterns in HTML text
  4. Look for doctor/owner names and construct firstname@domain patterns
  5. Construct common patterns (info@, contact@, office@, hello@)

Returns emails ranked by quality: named inbox > department > generic.
"""
import re
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

EMAIL_RE = re.compile(
    r"(?<![a-zA-Z0-9._%+-])"                          # no word char before
    r"([a-zA-Z0-9._%+-]{2,}@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
    r"(?![a-zA-Z0-9._%+-])"                           # no word char after
)

# Addresses to deprioritize (usually shared inboxes)
GENERIC_PREFIXES = {
    "info", "support", "noreply", "no-reply", "hello", "admin",
    "webmaster", "postmaster", "help", "sales", "contact",
    "office", "team", "feedback", "inquiries", "mail",
}

# Addresses to reject entirely (CDN/service emails, not the business)
REJECTED_PATTERNS = [
    r"@(sentry|cloudflare|amazonaws|googleapis|google-analytics|wixpress|squarespace|shopify|hubspot|intercom|mailchimp|sendgrid|stripe|twilio|wordpress|elementor|wp-engine)",
    r"^(example|test|demo|foo|bar|placeholder)@",
    r"@example\.(com|org|net)$",
    r"\.(png|jpg|jpeg|gif|svg|webp|pdf|zip)@",  # emails embedded in filenames
    r"@\d+x\d+",                                # image dimension strings
    r"^(and|the|or|for|you|your|our|this|that|from|with|have|will|into|over)@",  # common English words
    r"^\d+[a-z]?@",                             # starts with digit (usually IDs)
]

# Pages to scrape for contact info (in order of priority)
CONTACT_PATHS = [
    "",            # homepage
    "contact",
    "contact-us",
    "contact_us",
    "about",
    "about-us",
    "about_us",
    "team",
    "our-team",
    "meet-the-team",
    "staff",
    "doctors",
    "providers",
    "leadership",
    "our-doctors",
]

# Look for doctor/owner names on the page (for drname@ pattern construction)
PERSON_TITLE_RE = re.compile(
    r"\b(Dr\.?|Doctor|Attorney|CEO|Owner|Founder|Principal)\s+"
    r"([A-Z][a-zA-Z\-']+)(?:\s+([A-Z][a-zA-Z\-']+))?",
    re.IGNORECASE,
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

TIMEOUT = 8


def _normalize_url(url: str) -> str:
    """Ensure URL has scheme + drop trailing slash."""
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip("/")


def _extract_domain(website: str) -> str:
    if not website:
        return ""
    if not website.startswith(("http://", "https://")):
        website = "https://" + website
    try:
        netloc = urlparse(website).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return ""


def _is_rejected(email: str) -> bool:
    email_lower = email.lower()
    for pat in REJECTED_PATTERNS:
        if re.search(pat, email_lower):
            return True
    return False


def _extract_from_html(html: str) -> tuple:
    """Return (emails, person_names) found in the HTML."""
    emails = []
    person_names = []

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return [], []

    # mailto: links (highest signal)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("mailto:"):
            email = href.split(":", 1)[1].split("?", 1)[0].strip()
            if email and "@" in email and not _is_rejected(email):
                emails.append(email.lower())

    # Regex sweep on full HTML (catches obfuscated emails with mailto
    # variants and plain-text mentions)
    for match in EMAIL_RE.findall(html):
        if not _is_rejected(match):
            emails.append(match.lower())

    # Extract person names (for firstname@domain construction)
    text = soup.get_text(" ", strip=True)
    for m in PERSON_TITLE_RE.finditer(text):
        title = m.group(1)
        first = m.group(2)
        last = m.group(3) or ""
        person_names.append({
            "title": title,
            "first": first,
            "last": last,
            "full": f"{title} {first} {last}".strip(),
        })

    return emails, person_names


def _fetch(url: str) -> str:
    """Fetch a URL and return the HTML, or empty string on failure."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT,
                            allow_redirects=True)
        if resp.status_code >= 400:
            return ""
        return resp.text
    except requests.RequestException:
        return ""


def _rank_emails(emails: list, domain: str) -> list:
    """
    Rank emails by quality:
      1. Same-domain named inbox (e.g. gavin@domain.com)
      2. Same-domain generic (info@, contact@)
      3. Different-domain emails (rare for businesses, usually personal)
    Returns deduped list with highest-quality first.
    """
    seen = set()
    ranked = []

    # Tier 1: same-domain named
    same_domain_named = []
    same_domain_generic = []
    other_domain = []

    for e in emails:
        if e in seen:
            continue
        seen.add(e)
        local, _, dom = e.partition("@")
        prefix = local.lower().split(".")[0].split("+")[0]

        if domain and dom.endswith(domain):
            if prefix in GENERIC_PREFIXES:
                same_domain_generic.append(e)
            else:
                same_domain_named.append(e)
        else:
            other_domain.append(e)

    return same_domain_named + same_domain_generic + other_domain


# Common English stopwords that get mistaken for names when PERSON_TITLE_RE
# sees patterns like "Owner And Operator" or "Dr. The Best"
_NAME_STOPWORDS = {
    "and", "or", "the", "our", "your", "his", "her", "their",
    "for", "with", "from", "this", "that", "than", "then",
    "also", "plus", "new", "all", "any", "both", "each",
    "is", "was", "are", "were", "be", "been",
}


def _construct_patterns(domain: str, person: dict = None) -> list:
    """
    Construct common email patterns from a domain + optional person name.
    Returns a list of candidate emails ordered by likelihood.
    """
    if not domain:
        return []

    candidates = []

    if person and person.get("first"):
        first = person["first"].lower().strip()
        last = person.get("last", "").lower().strip()
        # Skip stopword-based false positives
        if first in _NAME_STOPWORDS or (last and last in _NAME_STOPWORDS):
            first = ""
        if first and len(first) >= 3 and first.isalpha():
            if last:
                candidates.extend([
                    f"{first}@{domain}",
                    f"{first}.{last}@{domain}",
                    f"{first[0]}{last}@{domain}",
                    f"{first}{last}@{domain}",
                ])
            else:
                candidates.append(f"{first}@{domain}")

    # Generic fallback patterns
    candidates.extend([
        f"info@{domain}",
        f"contact@{domain}",
        f"hello@{domain}",
        f"office@{domain}",
    ])

    return candidates


def scrape_business_emails(business_name: str, website: str,
                           include_constructed: bool = True) -> dict:
    """
    Scrape emails from a business website. Returns:
    {
        "scraped_emails": [...],          # actually found on website
        "constructed_emails": [...],      # pattern-based guesses
        "contact_names": [...],           # doctor/owner names found
        "primary_email": "..."            # best guess (highest-ranked)
        "website_accessible": bool,
        "pages_scraped": int,
    }
    """
    result = {
        "business_name": business_name,
        "website": website,
        "scraped_emails": [],
        "constructed_emails": [],
        "contact_names": [],
        "primary_email": "",
        "website_accessible": False,
        "pages_scraped": 0,
    }

    if not website:
        return result

    website = _normalize_url(website)
    domain = _extract_domain(website)
    result["domain"] = domain

    all_emails = []
    all_persons = []
    pages_scraped = 0

    # Fetch homepage first to confirm site is live
    homepage = _fetch(website)
    if not homepage:
        return result
    result["website_accessible"] = True
    pages_scraped += 1
    emails, persons = _extract_from_html(homepage)
    all_emails.extend(emails)
    all_persons.extend(persons)

    # Extract candidate page URLs from homepage links (some sites have
    # non-standard contact page URLs like /get-in-touch)
    candidate_paths = list(CONTACT_PATHS)
    try:
        soup = BeautifulSoup(homepage, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            link_text = a.get_text(" ", strip=True).lower()
            if any(kw in link_text for kw in ("contact", "about", "team", "staff")):
                # Extract the path from the href
                if href.startswith("/"):
                    candidate_paths.append(href.lstrip("/"))
                elif href.startswith(website):
                    candidate_paths.append(href.replace(website, "").lstrip("/"))
    except Exception:
        pass

    # Dedupe candidate paths
    candidate_paths = list(dict.fromkeys(candidate_paths))[:12]

    # Fetch candidate pages (skip the empty string which is homepage)
    for path in candidate_paths:
        if not path:
            continue
        url = urljoin(website + "/", path)
        html = _fetch(url)
        if html:
            pages_scraped += 1
            emails, persons = _extract_from_html(html)
            all_emails.extend(emails)
            all_persons.extend(persons)

    result["pages_scraped"] = pages_scraped

    # Dedupe + rank scraped emails
    ranked = _rank_emails(all_emails, domain)
    result["scraped_emails"] = ranked

    # Dedupe persons by full name
    seen_names = set()
    uniq_persons = []
    for p in all_persons:
        key = p["full"].lower()
        if key not in seen_names:
            seen_names.add(key)
            uniq_persons.append(p)
    result["contact_names"] = uniq_persons[:5]

    # Constructed email candidates
    if include_constructed and domain:
        constructed = []
        for person in uniq_persons[:3]:
            constructed.extend(_construct_patterns(domain, person))
        if not constructed:
            constructed = _construct_patterns(domain)
        # Dedupe, exclude already-scraped
        scraped_set = set(ranked)
        constructed = [c for c in dict.fromkeys(constructed) if c not in scraped_set]
        result["constructed_emails"] = constructed[:8]

    # Primary email = best scraped, or first constructed
    if ranked:
        result["primary_email"] = ranked[0]
    elif result["constructed_emails"]:
        result["primary_email"] = result["constructed_emails"][0]

    return result
