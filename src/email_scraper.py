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

# Addresses to deprioritize (usually shared inboxes, not decision makers)
GENERIC_PREFIXES = {
    "info", "support", "noreply", "no-reply", "hello", "admin",
    "webmaster", "postmaster", "help", "sales", "contact",
    "office", "team", "feedback", "inquiries", "mail",
    "customer_service", "customerservice", "customer-service",
    "customercare", "customer", "service", "care", "billing",
    "accounts", "accounting", "press", "media", "pr", "hr",
    "careers", "jobs", "marketing", "orders", "booking",
    "reservations", "reception", "frontdesk", "front-desk",
    "appointments",
}

# Addresses to reject entirely (CDN/service emails, not the business)
REJECTED_PATTERNS = [
    r"@(sentry|cloudflare|amazonaws|googleapis|google-analytics|wixpress|squarespace|shopify|hubspot|intercom|mailchimp|sendgrid|stripe|twilio|wordpress|elementor|wp-engine)",
    r"^(example|test|demo|foo|bar|placeholder)@",
    r"@example\.(com|org|net)$",
    # File asset references accidentally matched as emails
    r"\.(png|jpg|jpeg|gif|svg|webp|pdf|zip|ico|bmp|tiff)@",   # email embedded in filename
    r"@[^@]*\.(png|jpg|jpeg|gif|svg|webp|pdf|zip|ico|bmp|tiff|css|js|json|xml)$",  # filename AFTER @
    r"@\d+x(\.|$)",                                            # image dimension suffix like @2x.png
    r"@\d+x\d+",                                               # image dimension strings
    # Sprite/asset naming conventions
    r"(sprite|asset|icon|logo|bundle|chunk|hash|placeholder)-",
    # Common English words as prefix (false positives from page copy)
    r"^(and|the|or|for|you|your|our|this|that|from|with|have|will|into|over)@",
    r"^\d+[a-z]?@",                                            # starts with digit (usually IDs)
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

# Look for doctor/owner names. Title keywords use inline (?i:...) so case
# doesn't matter for the title, but the name tokens MUST start with a capital
# letter — this prevents lowercase phrases like "partner network community"
# from matching as "(Partner) (network) (community)".
PERSON_TITLE_RE = re.compile(
    r"\b((?i:Dr\.?|Doctor|Attorney|Lawyer|CEO|Owner|Founder|Co-?Founder|"
    r"President|Managing\s+Partner|Managing\s+Director|Principal|Partner|"
    r"Director|DDS|DMD|MD))\s+"
    r"([A-Z][a-zA-Z\-']+)(?:\s+([A-Z][a-zA-Z\-']+))?"
)

# Decision-maker title words — used to rank which extracted person is
# most likely the decision maker.
DECISION_TITLE_KEYWORDS = {
    "owner", "ceo", "chief executive", "founder", "co-founder", "cofounder",
    "president", "managing partner", "managing director", "principal",
    "practice owner", "partner",
}

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


# Department/role keywords that, if they appear as a substring of the
# email's local part, indicate it's a shared inbox (not a decision maker).
# Example: `wccdcustomerservice@` -> generic, even though the full string
# isn't in GENERIC_PREFIXES. Same for `abcsales@`, `info123@`, etc.
GENERIC_SUBSTRINGS = (
    "customerservice", "customercare", "customer-service", "customer_service",
    "customersupport", "clientservice", "clientcare",
    "billing", "accounts", "accounting",
    "sales", "marketing", "help", "support",
    "info", "contact", "hello", "service",
    "reservations", "booking", "appointments",
    "reception", "frontdesk", "front-desk",
    "press", "media", "pr",
    "hr", "careers", "jobs", "recruiting",
    "feedback", "inquiries", "inquiry",
    "noreply", "no-reply", "donotreply",
    "webmaster", "postmaster", "admin",
    "office", "team", "mail",
)


def _is_generic_inbox(email: str) -> bool:
    """Return True if the local part contains a known department/role word."""
    if not email or "@" not in email:
        return True
    local = email.partition("@")[0].lower()
    # Exact match against GENERIC_PREFIXES
    prefix = local.split(".")[0].split("+")[0]
    if prefix in GENERIC_PREFIXES:
        return True
    # Substring match (catches wccdcustomerservice, abcsales, etc.)
    for kw in GENERIC_SUBSTRINGS:
        if kw in local:
            return True
    return False


# Rank page URL paths by how authoritative "this is a real person at this
# business" signal is. Higher = stronger signal.
PAGE_AUTHORITY = {
    "team": 10, "our-team": 10, "meet-the-team": 10, "our_team": 10,
    "staff": 9, "doctors": 9, "our-doctors": 9, "providers": 9,
    "leadership": 10,
    "about": 7, "about-us": 7, "about_us": 7,
    "contact": 5, "contact-us": 5, "contact_us": 5,
    "": 3,   # homepage
}


def _page_authority(url_path: str) -> int:
    path = (url_path or "").strip("/").lower().split("/")[-1]
    return PAGE_AUTHORITY.get(path, 4)


def _extract_from_html(html: str, page_path: str = "") -> tuple:
    """
    Return (emails, person_names) found in the HTML.
    Each person now carries:
      - found_on: the URL path they were found on
      - authority: integer signal (team page > about > contact > homepage)
    """
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

    # Regex sweep on full HTML
    for match in EMAIL_RE.findall(html):
        if not _is_rejected(match):
            emails.append(match.lower())

    # Extract person names with page source
    authority = _page_authority(page_path)
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
            "found_on": page_path or "",
            "authority": authority,
        })

    return emails, person_names


def _detect_email_pattern(scraped_emails: list, domain: str) -> str:
    """
    Sniff the email pattern used by the business from scraped addresses.
    Returns a pattern string we can use to construct new emails:
      - 'first'         -> jane@domain.com
      - 'first.last'    -> jane.smith@domain.com
      - 'firstlast'     -> janesmith@domain.com
      - 'f.last'        -> j.smith@domain.com
      - 'flast'         -> jsmith@domain.com
      - ''              -> no clear pattern
    """
    if not scraped_emails or not domain:
        return ""
    for e in scraped_emails:
        local, _, dom = e.partition("@")
        if not dom.endswith(domain):
            continue
        if _is_generic_inbox(e):
            continue
        local = local.lower()
        if "." in local:
            parts = local.split(".")
            if len(parts) == 2 and all(p.isalpha() for p in parts):
                if len(parts[0]) == 1:
                    return "f.last"
                return "first.last"
        elif local.isalpha():
            if 2 <= len(local) <= 12:
                # Could be "first" (jane) or "flast" (jsmith) — ambiguous
                # Default to "first" (most common for small businesses)
                return "first"
    return ""


def _build_email_from_pattern(first: str, last: str, domain: str, pattern: str) -> str:
    first = (first or "").lower().strip()
    last = (last or "").lower().strip()
    if not first or not first.isalpha() or not domain:
        return ""
    if pattern == "first.last" and last:
        return f"{first}.{last}@{domain}"
    if pattern == "firstlast" and last:
        return f"{first}{last}@{domain}"
    if pattern == "f.last" and last:
        return f"{first[0]}.{last}@{domain}"
    if pattern == "flast" and last:
        return f"{first[0]}{last}@{domain}"
    return f"{first}@{domain}"


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

        if domain and dom.endswith(domain):
            # Use substring-based generic check so company-prefixed dept
            # inboxes (wccdcustomerservice@, abcsales@, etc.) also rank low
            if _is_generic_inbox(e):
                same_domain_generic.append(e)
            else:
                same_domain_named.append(e)
        else:
            other_domain.append(e)

    return same_domain_named + same_domain_generic + other_domain


# Common English stopwords that get mistaken for names when PERSON_TITLE_RE
# sees patterns like "Owner And Operator" or "Partner Network Community"
_NAME_STOPWORDS = {
    # Generic English
    "and", "or", "the", "our", "your", "his", "her", "their",
    "for", "with", "from", "this", "that", "than", "then",
    "also", "plus", "new", "all", "any", "both", "each",
    "is", "was", "are", "were", "be", "been",
    # Business/org words that follow title keywords
    "network", "services", "team", "group", "community", "program",
    "solutions", "partners", "networks", "office", "company", "llc",
    "inc", "corp", "agency", "firm", "practice", "clinic",
    "council", "board", "committee", "division", "department",
    "relations", "development", "marketing", "operations", "strategy",
    # Roles/titles that get matched as name tokens (false positives)
    "director", "manager", "associate", "analyst", "counsel", "lead",
    "executive", "specialist", "coordinator", "administrator", "assistant",
    "engineer", "consultant", "representative", "officer", "advisor",
    "legal", "finance", "sales", "product", "account", "client",
    "technical", "general", "senior", "junior", "chief", "vice",
    "global", "regional", "national", "international", "americas",
    "europe", "asia", "pacific",
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


def _is_decision_title(title: str) -> bool:
    if not title:
        return False
    tl = title.lower()
    return any(kw in tl for kw in DECISION_TITLE_KEYWORDS)


def _name_match(website_person: dict, linkedin_person: dict) -> bool:
    """Fuzzy match a website-extracted name to a LinkedIn-found name."""
    if not website_person or not linkedin_person:
        return False
    w_first = (website_person.get("first") or "").lower()
    w_last = (website_person.get("last") or "").lower()
    l_first = (linkedin_person.get("first") or "").lower()
    l_last = (linkedin_person.get("last") or "").lower()
    if not w_first or not l_first:
        return False
    # First name must match; last name matches if present on either side
    if w_first != l_first:
        return False
    if w_last and l_last and w_last != l_last:
        return False
    return True


def _pick_top_contact(scraped_emails: list, constructed_emails: list,
                      persons: list, linkedin_people: list, domain: str) -> dict:
    """
    Rank contacts and pick the single best one, with cross-verification.

    Confidence tiers:
      🟢 HIGH
        - Named personal scraped email (gavin@domain), OR
        - Website decision maker ON /team or /leadership page CROSS-VERIFIED by LinkedIn
      🟡 MEDIUM
        - Website decision maker ON /team or /leadership page (no LinkedIn match)
        - LinkedIn decision maker (not cross-verified)
        - Scraped email + decision-maker detected nearby
      🔴 LOW
        - Website person from /about or homepage only
        - Generic inbox fallback
    """
    contact = {
        "contact_name": "",
        "contact_title": "",
        "contact_email": "",
        "email_source": "",
        "confidence": "",
    }

    pattern = _detect_email_pattern(scraped_emails, domain)

    def build(first, last):
        if pattern:
            return _build_email_from_pattern(first, last, domain, pattern)
        return f"{(first or '').lower()}@{domain}" if first else ""

    # ── Tier 1a: Named personal scraped email ───────────────────────────
    for e in scraped_emails:
        if e and not _is_generic_inbox(e):
            contact["contact_email"] = e
            local = e.partition("@")[0].lower().split(".")[0]
            contact["email_source"] = "scraped_mailto_or_regex"
            contact["confidence"] = "high"
            # Attach name if we can
            first_guess = local.split(".")[0]
            for p in (linkedin_people or []) + (persons or []):
                if p.get("first", "").lower().startswith(first_guess[:4]):
                    contact["contact_name"] = p.get("name") or p.get("full", "")
                    contact["contact_title"] = p.get("title", "")
                    break
            return contact

    # ── Tier 1b: Website decision maker on /team page CROSS-VERIFIED ────
    for p in (persons or []):
        title = p.get("title", "")
        authority = p.get("authority", 0)
        if not _is_decision_title(title):
            continue
        if authority < 9:  # Must be on /team /doctors /leadership
            continue
        first = (p.get("first") or "").lower()
        last = (p.get("last") or "").lower()
        if first in _NAME_STOPWORDS or (last and last in _NAME_STOPWORDS):
            continue
        if not (first and first.isalpha() and len(first) >= 3 and domain):
            continue
        # Cross-verify with LinkedIn
        verified = any(_name_match(p, lp) for lp in (linkedin_people or []))
        email = build(first, last)
        if not email:
            continue
        contact["contact_email"] = email
        contact["contact_name"] = p.get("full", "")
        contact["contact_title"] = title
        if verified:
            contact["email_source"] = "team_page_verified_by_linkedin"
            contact["confidence"] = "high"
        else:
            contact["email_source"] = "team_page_decision_maker"
            contact["confidence"] = "medium"
        return contact

    # ── Tier 2: LinkedIn decision maker CROSS-VERIFIED by website name ──
    for lp in (linkedin_people or []):
        if not _is_decision_title(lp.get("title", "")):
            continue
        first = (lp.get("first") or "").lower()
        last = (lp.get("last") or "").lower()
        if not (first and first.isalpha() and len(first) >= 2 and domain):
            continue
        verified = any(_name_match(wp, lp) for wp in (persons or []))
        email = build(first, last)
        if not email:
            continue
        contact["contact_email"] = email
        contact["contact_name"] = lp.get("name", "")
        contact["contact_title"] = lp.get("title", "")
        if verified:
            contact["email_source"] = "linkedin_verified_by_website"
            contact["confidence"] = "high"
        else:
            contact["email_source"] = "constructed_from_linkedin"
            contact["confidence"] = "medium"
        return contact

    # ── Tier 3: Website decision maker on /about or homepage ────────────
    for p in (persons or []):
        title = p.get("title", "")
        if not _is_decision_title(title):
            continue
        first = (p.get("first") or "").lower()
        last = (p.get("last") or "").lower()
        if first in _NAME_STOPWORDS or (last and last in _NAME_STOPWORDS):
            continue
        if not (first and first.isalpha() and len(first) >= 3 and domain):
            continue
        email = build(first, last)
        if not email:
            continue
        contact["contact_email"] = email
        contact["contact_name"] = p.get("full", "")
        contact["contact_title"] = title
        contact["email_source"] = "constructed_from_website_decision_maker"
        contact["confidence"] = "medium"
        return contact

    # ── Tier 4: Any person from /team (Dr./Doctor — common for dental) ──
    for p in (persons or []):
        authority = p.get("authority", 0)
        if authority < 9:  # require team/leadership page
            continue
        first = (p.get("first") or "").lower()
        last = (p.get("last") or "").lower()
        if first in _NAME_STOPWORDS or (last and last in _NAME_STOPWORDS):
            continue
        if not (first and first.isalpha() and len(first) >= 3 and domain):
            continue
        email = build(first, last)
        if not email:
            continue
        contact["contact_email"] = email
        contact["contact_name"] = p.get("full", "")
        contact["contact_title"] = p.get("title", "")
        contact["email_source"] = "team_page_person"
        contact["confidence"] = "medium"
        return contact

    # ── Tier 5: Any named person from lower-authority pages ─────────────
    for p in (persons or []):
        first = (p.get("first") or "").lower()
        last = (p.get("last") or "").lower()
        if first in _NAME_STOPWORDS or (last and last in _NAME_STOPWORDS):
            continue
        if not (first and first.isalpha() and len(first) >= 3 and domain):
            continue
        email = build(first, last)
        if not email:
            continue
        contact["contact_email"] = email
        contact["contact_name"] = p.get("full", "")
        contact["contact_title"] = p.get("title", "")
        contact["email_source"] = "constructed_from_website_name"
        contact["confidence"] = "low"
        return contact

    # ── Tier 6: Generic fallback ────────────────────────────────────────
    for e in scraped_emails + constructed_emails:
        contact["contact_email"] = e
        contact["email_source"] = "generic_inbox"
        contact["confidence"] = "low"
        return contact

    return contact


def scrape_business_emails(business_name: str, website: str,
                           include_constructed: bool = True,
                           find_decision_makers: bool = True,
                           location: str = "") -> dict:
    """
    Scrape emails from a business website + find decision makers via LinkedIn.

    Returns:
    {
        "scraped_emails": [...],          # found on website
        "constructed_emails": [...],      # pattern-based guesses
        "contact_names": [...],           # website-extracted names
        "linkedin_people": [...],         # LinkedIn decision makers
        "primary_email": "..."            # single best email
        "contact_name": "...",            # decision maker's name (if found)
        "contact_title": "...",           # their title
        "email_source": "...",            # where the email came from
        "confidence": "high|medium|low",  # our confidence in this email
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
        "linkedin_people": [],
        "primary_email": "",
        "contact_name": "",
        "contact_title": "",
        "email_source": "",
        "confidence": "",
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
    emails, persons = _extract_from_html(homepage, page_path="")
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
            emails, persons = _extract_from_html(html, page_path=path)
            all_emails.extend(emails)
            all_persons.extend(persons)

    result["pages_scraped"] = pages_scraped

    # Dedupe + rank scraped emails
    ranked = _rank_emails(all_emails, domain)
    result["scraped_emails"] = ranked

    # Dedupe persons by full name, keeping the highest-authority occurrence
    seen_names = {}
    for p in all_persons:
        key = p["full"].lower()
        if key not in seen_names or p.get("authority", 0) > seen_names[key].get("authority", 0):
            seen_names[key] = p
    # Sort by authority DESC (team page first, then about, then contact, then homepage)
    uniq_persons = sorted(seen_names.values(), key=lambda p: -p.get("authority", 0))
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

    # LinkedIn decision-maker lookup (1 SearchApi credit per business)
    linkedin_people = []
    if find_decision_makers and business_name:
        try:
            from src.people_finder import find_decision_makers as _find_dm
            linkedin_people = _find_dm(business_name, location) or []
        except Exception:
            linkedin_people = []
    result["linkedin_people"] = linkedin_people

    # Pick the single best contact (name + email + source + confidence)
    top = _pick_top_contact(
        scraped_emails=ranked,
        constructed_emails=result["constructed_emails"],
        persons=uniq_persons,
        linkedin_people=linkedin_people,
        domain=domain,
    )
    result["primary_email"] = top["contact_email"]
    result["contact_name"] = top["contact_name"]
    result["contact_title"] = top["contact_title"]
    result["email_source"] = top["email_source"]
    result["confidence"] = top["confidence"]

    return result
