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
    r"([a-zA-Z0-9]"                                   # FIRST char must be alphanumeric (RFC-valid)
    r"[a-zA-Z0-9._%+-]{1,}@"
    r"[a-zA-Z0-9]"                                    # FIRST char of domain must be alphanumeric (not -)
    r"[a-zA-Z0-9.-]*\.[a-zA-Z]{2,})"
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

# Template/placeholder local parts. These appear verbatim in contact-form
# HTML attributes ("placeholder=first@yourdomain.com"), JS email-assembly
# snippets (`'first' + '@' + domain`), and sample text in page templates.
# Any email whose local part exactly matches one of these is a placeholder,
# not a real mailbox.
PLACEHOLDER_LOCALS = {
    "first", "last", "firstname", "lastname", "fname", "lname",
    "your", "you", "youremail", "your-email", "your_email",
    "name", "username", "user", "email", "mail",
    "example", "example1", "example2", "sample", "template",
    "placeholder", "changeme", "yourname", "yourcompany",
    "domain", "yourdomain", "here", "youraddress",
}

# Addresses to reject entirely (CDN/service emails, not the business)
REJECTED_PATTERNS = [
    r"@(sentry|cloudflare|amazonaws|googleapis|google-analytics|wixpress|squarespace|shopify|hubspot|intercom|mailchimp|sendgrid|stripe|twilio|wordpress|elementor|wp-engine)",
    r"^(example|test|demo|foo|bar|placeholder)@",
    r"@example\.(com|org|net)$",
    # File asset references accidentally matched as emails
    r"\.(png|jpg|jpeg|gif|svg|webp|pdf|zip|ico|bmp|tiff|ttf|woff|woff2|otf|eot)@",
    r"@[^@]*\.(png|jpg|jpeg|gif|svg|webp|pdf|zip|ico|bmp|tiff|ttf|woff|woff2|otf|eot|css|js|json|xml|font)$",
    r"@-?[a-z]*\.(ttf|woff|woff2|otf|eot)",          # font filename after @ (e.g. @-regular.ttf)
    r"@\d+x(\.|$)",                                            # image dimension suffix like @2x.png
    r"@\d+x\d+",                                               # image dimension strings
    # Sprite/asset naming conventions
    r"(sprite|asset|icon|logo|bundle|chunk|hash|placeholder)-",
    # Font names commonly embedded in CSS (Montserrat, Roboto, etc. + variant)
    r"@-(regular|bold|italic|medium|light|thin|black|semibold|extrabold)",
    # Common English words as prefix (false positives from page copy)
    r"^(and|the|or|for|you|your|our|this|that|from|with|have|will|into|over)@",
    r"^\d+[a-z]?@",                                            # starts with digit (usually IDs)
    # Domain starts with a hyphen (never valid)
    r"@-",
    # Local part starts with a dot or dash (never valid per RFC)
    r"^[.-]",
    # Consecutive dots or dot-before-@ (malformed)
    r"\.@",
    r"\.\.",
    # Suspicious schema.org / code-like artifacts leaked into scraping
    r"(status|state|current|context|type|id|graph|keyframes|import|media)@",
    # Fake TLDs that are actually code identifiers or filenames (not real public TLDs)
    r"\.(init|config|conf|local|test|invalid|example|lan|internal|"
    r"params|args|props|state|ttf|woff|woff2|otf|eot|font|css|js|json|"
    r"xml|html|htm|svg|png|jpg|jpeg|gif|pdf)$",
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
    # Placeholder local-parts ("first@domain", "youremail@domain", etc.)
    if "@" in email_lower:
        local = email_lower.split("@", 1)[0]
        if local in PLACEHOLDER_LOCALS:
            return True
        # Strip one trailing numeric suffix (example1 → example, name2 → name)
        stripped = re.sub(r"\d+$", "", local)
        if stripped and stripped in PLACEHOLDER_LOCALS:
            return True
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

    # Enhanced hidden-email sources: Cloudflare, obfuscated [at], JSON-LD,
    # JS-assembled, meta tags. Each catches a different obfuscation pattern.
    try:
        from src.email_sources import extract_all_hidden_emails
        hidden = extract_all_hidden_emails(html)
        for source_name, source_emails in hidden.items():
            for e in source_emails:
                if e and not _is_rejected(e) and e not in emails:
                    emails.append(e)
    except Exception:
        pass

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
    Returns a pattern string we can use to construct new emails.

    Uses the legacy single-email heuristic. For stronger signal from
    multiple emails, use _detect_email_pattern_multi() which returns a
    confidence-weighted result.
    """
    multi = _detect_email_pattern_multi(scraped_emails, domain)
    return multi.get("pattern", "")


def _detect_email_pattern_multi(scraped_emails: list, domain: str) -> dict:
    """
    Sniff the email pattern using MULTIPLE scraped emails for higher
    confidence. If an org uses 'first.last@' for 3 different people, we're
    very confident that's their convention — apply it to all constructed
    emails for that domain.

    Returns:
      {
        "pattern": "first.last" | "first" | "flast" | "f.last" | "firstlast" | "",
        "confidence": "high" | "medium" | "low" | "none",
        "evidence_count": <int — how many real emails confirmed this pattern>,
        "all_patterns": {"first.last": 3, "first": 1, ...}  # raw counts
      }
    """
    result = {"pattern": "", "confidence": "none", "evidence_count": 0,
              "all_patterns": {}}
    if not scraped_emails or not domain:
        return result

    pattern_counts = {}
    for e in scraped_emails:
        local, _, dom = e.partition("@")
        if not dom or not dom.endswith(domain):
            continue
        if _is_generic_inbox(e):
            continue
        local_lower = local.lower()

        # Classify this email's pattern
        pat = _classify_local_part(local_lower)
        if pat:
            pattern_counts[pat] = pattern_counts.get(pat, 0) + 1

    result["all_patterns"] = pattern_counts
    if not pattern_counts:
        return result

    # Pick the winning pattern (most evidence)
    winner = max(pattern_counts.items(), key=lambda x: x[1])
    pattern, count = winner
    result["pattern"] = pattern
    result["evidence_count"] = count

    # Confidence: >=3 examples = high, 2 = medium, 1 = low
    if count >= 3:
        result["confidence"] = "high"
    elif count == 2:
        result["confidence"] = "medium"
    else:
        result["confidence"] = "low"

    return result


def _classify_local_part(local: str) -> str:
    """Return the pattern name for a local part, or '' if ambiguous/unusable."""
    if not local or not local.replace(".", "").replace("-", "").isalpha():
        return ""

    if "." in local:
        parts = local.split(".")
        if len(parts) == 2 and all(p.isalpha() for p in parts):
            if len(parts[0]) == 1:
                return "f.last"
            if len(parts[0]) >= 2 and len(parts[1]) >= 2:
                return "first.last"
    elif local.isalpha():
        # Ambiguous between 'first' (jane) and 'flast' (jsmith) and
        # 'firstlast' (janesmith). We can't reliably distinguish without
        # knowing the person's real name. Default to 'first' which is most
        # common for small businesses.
        if 2 <= len(local) <= 12:
            return "first"
    return ""


def _detect_email_pattern_old(scraped_emails: list, domain: str) -> str:
    """LEGACY: single-email detection. Kept for reference; not used."""
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


def _construct_patterns(domain: str, person: dict = None,
                         industry: str = "", headcount: int = None) -> list:
    """
    Construct common email patterns from a domain + optional person name.
    Returns a list of candidate emails ordered by industry-specific likelihood.

    When industry is set, uses src.industry_patterns.get_patterns_for() to
    pick the right order (e.g. dental practices get first.last@ first, not
    first@). Falls back to generic ordering when industry is unknown.
    """
    if not domain:
        return []

    candidates = []

    if person and person.get("first"):
        first = person["first"].lower().strip()
        last = (person.get("last") or "").lower().strip()
        # Skip stopword-based false positives
        if first in _NAME_STOPWORDS or (last and last in _NAME_STOPWORDS):
            first = ""
        if first and len(first) >= 3 and first.isalpha():
            # Use industry-specific pattern priors when available
            try:
                from src.industry_patterns import get_patterns_for, build_email
                priors = get_patterns_for(industry, headcount)
                for pattern_name, _weight in priors:
                    email = build_email(pattern_name, first, last, domain)
                    if email and email not in candidates:
                        candidates.append(email)
            except Exception:
                # Fallback to hardcoded order
                if last:
                    candidates.extend([
                        f"{first}.{last}@{domain}",  # first.last@ now default
                        f"{first}@{domain}",
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


def _construct_patterns_with_labels(first, last, domain, industry="",
                                      headcount=None):
    """
    Industry-aware pattern construction that returns (email, pattern_name) tuples.
    Useful when you need to track WHICH pattern was used for each candidate
    (e.g. for bounce tracking and learning).

    Returns list of (email, pattern_name), highest-priority first.
    """
    if not first or not domain:
        return []
    try:
        from src.industry_patterns import get_patterns_for, build_email
    except Exception:
        return []
    priors = get_patterns_for(industry, headcount)
    out = []
    for pattern_name, _weight in priors:
        email = build_email(pattern_name, first, last, domain)
        if email:
            out.append((email, pattern_name))
    return out


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

    pattern_info = _detect_email_pattern_multi(scraped_emails, domain)
    pattern = pattern_info.get("pattern", "")
    pattern_confidence = pattern_info.get("confidence", "none")

    def build(first, last):
        if pattern:
            return _build_email_from_pattern(first, last, domain, pattern)
        # No detected pattern — default to first.last@ (dominant B2B prior for
        # 10+ employee practices) when we have both; otherwise fall back to
        # first@. Never fall back to info@ here — that's Tier 6, not a
        # person-specific email.
        first = (first or "").lower()
        last = (last or "").lower()
        if first and last:
            return f"{first}.{last}@{domain}"
        if first:
            return f"{first}@{domain}"
        return ""

    # ── Tier 1a: Named personal scraped email ───────────────────────────
    for e in scraped_emails:
        if not e or _is_generic_inbox(e) or _is_rejected(e):
            continue
        # Also reject obvious junk we may have let through other extractors
        local = e.partition("@")[0].lower()
        if len(local) < 2 or local.startswith(".") or local.startswith("-"):
            continue
        domain_part = e.partition("@")[2].lower()
        if not domain_part or domain_part.startswith("-") or domain_part.startswith("."):
            continue
        contact["contact_email"] = e
        contact["email_source"] = "scraped_mailto_or_regex"
        contact["confidence"] = "high"
        # Attach name if we can — prefer LinkedIn + NPI persons over generic regex matches
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
            # Tightened tier logic: MEDIUM requires two positive signals
            # (LinkedIn decision maker + pattern evidence from ≥2 emails).
            # Pure guess with no pattern evidence = LOW, not MEDIUM.
            if pattern_confidence == "high":
                contact["confidence"] = "high"
                contact["email_source"] += "_pattern_confirmed"
            elif pattern_confidence == "medium":
                contact["confidence"] = "medium"
                contact["email_source"] += "_pattern_anchored"
            else:
                contact["confidence"] = "low"
                contact["email_source"] += "_no_pattern_evidence"
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
        # Tightened tier logic: MEDIUM requires pattern evidence ≥ medium.
        # Without real pattern evidence, first@domain is a guess = LOW.
        if pattern_confidence == "high":
            contact["confidence"] = "high"
            contact["email_source"] += "_pattern_confirmed"
        elif pattern_confidence == "medium":
            contact["confidence"] = "medium"
            contact["email_source"] += "_pattern_anchored"
        else:
            contact["confidence"] = "low"
            contact["email_source"] += "_no_pattern_evidence"
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
                           location: str = "",
                           auto_verify: bool = False,
                           use_haiku_fallback: bool = False,
                           business_type: str = "",
                           address: str = "",
                           phone: str = "") -> dict:
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

    # ── HAIKU FALLBACK: if rule-based found <2 people, ask Haiku to extract ──
    haiku_used = {"extract_people": False, "match_emails": False,
                   "filter_false_positives": False}
    if use_haiku_fallback and len(uniq_persons) < 2 and homepage:
        try:
            from src.haiku_scraper import haiku_extract_people, is_haiku_available
            if is_haiku_available():
                # Concatenate team-page HTML (highest-authority pages)
                team_html = homepage
                for path, page_auth in [("team", 10), ("our-team", 10),
                                         ("about", 7), ("leadership", 10)]:
                    if path in candidate_paths:
                        url = urljoin(website + "/", path)
                        team_html += _fetch(url) or ""
                haiku_persons = haiku_extract_people(team_html, business_name)
                haiku_used["extract_people"] = len(haiku_persons) > 0
                # Merge into uniq_persons — tag source
                existing_names = {p["full"].lower() for p in uniq_persons}
                for hp in haiku_persons:
                    name = hp.get("name", "").strip()
                    if name and name.lower() not in existing_names:
                        uniq_persons.append({
                            "title": hp.get("title", ""),
                            "first": hp.get("first", ""),
                            "last": hp.get("last", ""),
                            "full": name,
                            "found_on": "haiku",
                            "authority": 9,  # Haiku extraction = near team-page auth
                        })
                        existing_names.add(name.lower())
                        # If Haiku also returned an email for this person
                        he = hp.get("email")
                        if he and he not in all_emails and not _is_rejected(he):
                            all_emails.append(he)
        except Exception as e:
            print(f"[email_scraper] Haiku extract_people failed: {e}", file=__import__('sys').stderr)

    # ── HAIKU FALLBACK: filter false positives if scraped list is noisy ──
    if use_haiku_fallback and len(all_emails) >= 5:
        try:
            from src.haiku_scraper import haiku_filter_false_positives
            before = set(all_emails)
            all_emails = haiku_filter_false_positives(all_emails)
            haiku_used["filter_false_positives"] = set(all_emails) != before
        except Exception:
            pass

    # Re-rank after potential Haiku filtering
    ranked = _rank_emails(all_emails, domain)
    result["scraped_emails"] = ranked

    result["contact_names"] = uniq_persons[:5]

    # Constructed email candidates
    if include_constructed and domain:
        constructed = []
        for person in uniq_persons[:3]:
            constructed.extend(_construct_patterns(
                domain, person, industry=business_type))
        if not constructed:
            constructed = _construct_patterns(domain, industry=business_type)
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

    # ── NPI / licensing lookup for regulated verticals ────────────────
    # Fires in Verified + Deep modes when we have business_type + address.
    # Returns authoritative provider names (Dr. names for dental/medical)
    # that anchor pattern construction with high confidence.
    licensed_people = []
    if auto_verify and business_type and address:
        try:
            from src.licensing_lookup import (
                lookup_licensed_providers, parse_location,
            )
            bt_lower = (business_type or "").lower()
            if any(k in bt_lower for k in ("dental", "dentist", "medical",
                                             "doctor", "chiropr", "physical")):
                city, state, postal, street = parse_location(address)
                if state:
                    licensed_people = lookup_licensed_providers(
                        vertical=bt_lower,
                        business_name=business_name,
                        city=city,
                        state=state,
                        street_address=street,
                        postal_code=postal,
                    ) or []
                    # Add to uniq_persons so pattern detection + contact
                    # picking sees these authoritative names
                    existing_names = {p.get("full", "").lower() for p in uniq_persons}
                    for lp in licensed_people:
                        if lp.get("name", "").lower() not in existing_names:
                            uniq_persons.append({
                                "title": lp.get("title", ""),
                                "first": lp.get("first", ""),
                                "last": lp.get("last", ""),
                                "full": lp.get("name", ""),
                                "found_on": lp.get("source", "npi_registry"),
                                "authority": lp.get("authority", 15),
                            })
        except Exception as e:
            import sys as _sys
            print(f"[email_scraper] NPI lookup error: {e}", file=_sys.stderr)
    result["licensed_providers"] = licensed_people

    # ── WHOIS cross-verification ─────────────────────────────────────
    whois_result = {"matches": None}
    if auto_verify and domain and phone:
        try:
            from src.whois_verifier import verify_against_business_phone
            whois_result = verify_against_business_phone(domain, phone)
        except Exception as e:
            import sys as _sys
            print(f"[email_scraper] WHOIS error: {e}", file=_sys.stderr)
    result["whois_result"] = whois_result

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

    # Expose pattern-detection evidence so the UI can show WHY we're confident
    pattern_info = _detect_email_pattern_multi(ranked, domain)
    result["email_pattern_info"] = pattern_info
    result["haiku_used"] = haiku_used if use_haiku_fallback else {}

    # ── SMTP PATTERN VERIFICATION ────────────────────────────────────────
    # Only runs for CONSTRUCTED emails (not scraped) AND when auto_verify
    # is enabled. Tries multiple patterns until one verifies, or marks the
    # business as SKIP if nothing deliverable is found.
    if auto_verify and result["primary_email"]:
        is_scraped = top.get("email_source", "").startswith("scraped")
        has_person = bool(result["contact_name"])
        # Pull first/last for pattern test
        first, last = "", ""
        if has_person:
            # Try to get from matching person
            for p in uniq_persons + linkedin_people:
                if p.get("name") == result["contact_name"] or p.get("full") == result["contact_name"]:
                    first = (p.get("first") or "").lower()
                    last = (p.get("last") or "").lower()
                    break
            # Fallback: parse from name
            if not first:
                parts = result["contact_name"].split()
                # Skip titles like Dr./Mr./Mrs.
                parts = [p for p in parts if not p.rstrip(".").lower() in
                          ("dr", "doctor", "mr", "mrs", "ms")]
                if parts:
                    first = parts[0].lower()
                    if len(parts) >= 2:
                        last = parts[-1].lower()

        # Skip SMTP for scraped personal emails — they're already trusted
        if not is_scraped and domain and first:
            try:
                from src.email_verifier import (
                    verify_smtp_patterns, STATUS_VALID, STATUS_INVALID,
                )
                smtp_result = verify_smtp_patterns(first, last, domain, timeout=8)
                smtp_status = smtp_result.get("status", "")
                result["smtp_verified"] = smtp_status
                result["smtp_verified_reason"] = smtp_result.get("reason", "")

                if smtp_status == STATUS_VALID:
                    # The verified email replaces our guess
                    result["primary_email"] = smtp_result["email"]
                    result["confidence"] = "high"
                    result["email_source"] = top["email_source"] + "_smtp_verified"
                elif smtp_status == STATUS_INVALID:
                    # All patterns bounced — kill the primary email
                    result["primary_email"] = ""
                    result["confidence"] = "skip"
                    result["email_source"] = "smtp_all_invalid_skip"
                    result["smtp_tried_patterns"] = smtp_result.get("patterns_tried", 0)
                else:
                    # UNKNOWN — server blocked probe. Apply tightened tier:
                    # keep if ANY positive signal exists: pattern_confidence >= medium,
                    # OR person came from NPI (authority>=15 is authoritative),
                    # OR person came from LinkedIn (cross-verified by website).
                    pat_conf = pattern_info.get("confidence", "none")
                    has_npi_person = any(
                        p.get("authority", 0) >= 15 or
                        (p.get("found_on") or "").startswith("npi")
                        for p in uniq_persons
                    )
                    has_linkedin_match = any(
                        _name_match(wp, lp) for wp in uniq_persons
                        for lp in linkedin_people
                    ) if linkedin_people else False

                    if has_person and pat_conf in ("medium", "high"):
                        result["confidence"] = "high" if has_npi_person else "medium"
                        result["email_source"] = top["email_source"] + "_unverified_with_signal"
                    elif has_person and has_npi_person:
                        # NPI alone is a strong signal — keep as medium
                        result["confidence"] = "medium"
                        result["email_source"] = top["email_source"] + "_npi_anchored"
                    elif has_person and has_linkedin_match:
                        result["confidence"] = "medium"
                        result["email_source"] = top["email_source"] + "_linkedin_cross_verified"
                    else:
                        result["primary_email"] = ""
                        result["confidence"] = "skip"
                        result["email_source"] = "smtp_unknown_no_signal_skip"
            except Exception as e:
                print(f"[email_scraper] SMTP verify failed: {e}",
                      file=__import__('sys').stderr)

    # ── WHOIS boost applied AFTER SMTP so both signals compose ────────
    if auto_verify and result.get("primary_email"):
        if whois_result.get("matches") is True:
            source = result.get("email_source", "")
            if result.get("confidence") == "high":
                result["email_source"] = f"{source}_whois_confirmed"
            elif result.get("confidence") == "medium":
                result["confidence"] = "high"
                result["email_source"] = f"{source}_whois_confirmed"
            elif result.get("confidence") == "low":
                result["confidence"] = "medium"
                result["email_source"] = f"{source}_whois_confirmed"
            result["whois_verified"] = True
        elif whois_result.get("matches") is False:
            source = result.get("email_source", "")
            result["email_source"] = f"{source}_whois_mismatch_warning"
            result["whois_mismatch"] = True

    return result


# ============================================================
# Triangulation entry point (v3 pipeline)
# ============================================================

def _describe_email_source(result) -> str:
    """
    Render a specific, human-readable description of how the winning email
    was chosen. Replaces the uninformative "triangulation" label in the
    CSV/DB so operators can tell at a glance whether the email is:
        - a triangulated pattern applied to the decision maker (strongest)
        - a scraped personal mailbox from the website
        - a scraped shared inbox (info@, contact@)
        - an industry-prior guess (weakest)
        - the generic first.last@ fallback
    Always suffixed with the NeverBounce verdict when available.
    """
    best = (result.best_email or "").lower()
    if not best:
        return "no_candidate_produced"

    # Find the candidate dict that matches best_email (pipeline sorts by
    # confidence desc, best_email is candidates[0] in the happy path).
    winner = None
    for c in (result.candidate_emails or []):
        if (c.get("email") or "").lower() == best:
            winner = c
            break
    if winner is None:
        winner = (result.candidate_emails or [{}])[0]

    local = best.split("@", 1)[0] if "@" in best else ""
    # Heuristic: does the local part look like a shared inbox vs a person?
    SHARED = {
        "info", "contact", "contactus", "hello", "hi", "team", "support",
        "admin", "office", "mail", "enquiries", "inquiries", "sales",
        "marketing", "help", "service", "reception", "frontdesk",
        "appointments", "bookings", "smile", "welcome", "intake",
    }
    is_shared = local in SHARED or any(local.startswith(p) for p in ("no-reply", "noreply"))

    # Does the local part reference the DM's name? (Simple substring check.)
    dm_local_match = False
    dm = getattr(result, "decision_maker", None)
    if dm:
        first = (dm.first_name or "").lower()
        last = (dm.last_name or "").lower()
        if (first and first in local) or (last and last in local):
            dm_local_match = True

    source = winner.get("source") or ""
    pattern = winner.get("pattern") or ""

    # Top-level path:
    if source == "detected_pattern":
        pat = getattr(result.detected_pattern, "pattern_name", pattern)
        method = getattr(result.detected_pattern, "method", "") or ""
        ev_n = len(getattr(result.detected_pattern, "evidence_emails", []) or [])
        if method == "triangulation" and ev_n >= 1:
            label = f"triangulated pattern '{pat}' (evidence: {ev_n} email{'s' if ev_n != 1 else ''})"
        else:
            label = f"detected pattern '{pat}'"
    elif source == "scraped_direct":
        if is_shared:
            label = "scraped from website (shared inbox)"
        elif dm_local_match:
            label = "scraped from website (decision maker mailbox)"
        else:
            label = "scraped from website (personal mailbox)"
    elif source == "industry_prior":
        label = f"industry prior '{pattern}' applied to decision maker"
    elif source == "first_last_fallback":
        label = "fallback first.last@ (no stronger signal)"
    else:
        label = source or "unknown_path"

    # NB verdict suffix
    nb = winner.get("nb_result")
    nb_label = ""
    if nb == "valid":
        nb_label = " — NeverBounce VALID"
    elif nb == "catchall":
        nb_label = " — NeverBounce CATCH-ALL (unverified)"
    elif nb == "invalid":
        nb_label = " — NeverBounce INVALID"
    elif nb == "unknown":
        nb_label = " — NeverBounce UNKNOWN"
    elif nb is None and winner.get("smtp_valid"):
        nb_label = " — SMTP accepted"

    # Flag if the top email is below the safe-to-send threshold
    if not getattr(result, "safe_to_send", False):
        nb_label += " [below threshold]"

    return label + nb_label


def scrape_with_triangulation(business: dict, use_neverbounce: bool = True,
                                confidence_threshold: int = 70) -> dict:
    """
    Universal triangulation entry point. Runs the v5 universal_pipeline
    (industry-agnostic owner discovery + SQLite cache + NeverBounce) and
    adapts the TriangulationResult into a scrape_result dict that
    storage.update_business_emails can persist.
    """
    import json as _json
    import re as _re
    from urllib.parse import urlparse as _urlparse
    from src.universal_pipeline import triangulate_email

    website = business.get("website", "") or ""
    try:
        domain = _urlparse(
            website if website.startswith("http") else "https://" + website
        ).netloc.replace("www.", "")
    except Exception:
        domain = ""

    result = triangulate_email(
        business_name=business.get("business_name", ""),
        website=website,
        domain=domain,
        address=business.get("address", "") or business.get("location", "") or "",
        industry=(business.get("business_type") or "").lower(),
        decision_maker_hint=business.get("contact_name") or None,
        scraped_emails=business.get("scraped_emails") or [],
        use_neverbounce=use_neverbounce,
        confidence_threshold=confidence_threshold,
    )

    # Map confidence bucket for display
    conf_int = result.best_email_confidence
    if conf_int >= 85:
        conf_bucket = "high"
    elif conf_int >= 60:
        conf_bucket = "medium"
    elif conf_int > 0:
        conf_bucket = "low"
    else:
        conf_bucket = ""

    # Decision-maker name (full) + title for contact fields. Universal
    # pipeline's OwnerCandidate uses `title` (e.g. "Owner", "DDS");
    # extract an NPI number from the NPI provider-view source_url when
    # the owner came from the NPI agent.
    dm_name = ""
    dm_title = ""
    dm_npi = None
    if result.decision_maker:
        candidate_name = result.decision_maker.full_name or ""
        # Last-line safety net: the universal_pipeline is supposed to
        # have filtered junk names, but if anything slipped through
        # (exception, old cache path, future regression), refuse to
        # write a junk name as the business's contact. The business
        # still gets saved — it just won't claim a fake owner.
        try:
            from src.universal_pipeline import _is_junk_name as _junk
            if _junk(candidate_name, business_name=business.get("business_name", "")):
                candidate_name = ""
                dm_title = ""
        except Exception:
            pass
        if candidate_name:
            dm_name = candidate_name
            dm_title = getattr(result.decision_maker, "title", "") or ""
            src_url = getattr(result.decision_maker, "source_url", "") or ""
            m = _re.search(r"/provider-view/(\d{10})", src_url)
            if m:
                dm_npi = m.group(1)

    def _npi_from_owner(o):
        su = getattr(o, "source_url", "") or ""
        mm = _re.search(r"/provider-view/(\d{10})", su)
        return mm.group(1) if mm else None

    # JSON payload for professional_ids column (evidence trail).
    # Schema is preserved across pipeline versions for backward compat.
    owners_list = getattr(result, "all_owners", None) or getattr(result, "all_providers", []) or []
    professional_ids = {
        "decision_maker": ({
            "name": result.decision_maker.full_name,
            "npi": dm_npi,
            "credential": dm_title,
            "source": getattr(result.decision_maker, "source", ""),
            "source_url": getattr(result.decision_maker, "source_url", ""),
        } if result.decision_maker else None),
        "all_providers": [
            {
                "name": p.full_name,
                "npi": _npi_from_owner(p),
                "credential": getattr(p, "title", "") or getattr(p, "credential", ""),
                "source": getattr(p, "source", ""),
            }
            for p in owners_list
        ],
        "detected_pattern": ({
            "pattern": result.detected_pattern.pattern_name,
            "confidence": result.detected_pattern.confidence,
            "method": result.detected_pattern.method,
            "evidence_emails": result.detected_pattern.evidence_emails,
            "evidence_names": result.detected_pattern.evidence_names,
        } if result.detected_pattern else None),
        "agents_run": result.agents_run,
        "agents_succeeded": result.agents_succeeded,
        "time_seconds": round(result.time_seconds, 2),
        "cost_estimate": round(getattr(result, "cost_estimate", 0.0), 4),
        "candidate_emails": [
            {k: c.get(k) for k in ("email", "pattern", "source", "confidence",
                                    "smtp_valid", "smtp_catchall", "nb_result")}
            for c in result.candidate_emails
        ],
        "risky_catchall": getattr(result, "risky_catchall", False),
    }

    pattern_name = result.detected_pattern.pattern_name if result.detected_pattern else (
        result.candidate_emails[0].get("pattern") if result.candidate_emails else None
    )
    method = result.detected_pattern.method if result.detected_pattern else "industry_fallback"

    # Describe the SPECIFIC path that produced the winning email so operators
    # can see at a glance how much to trust it — not just the generic
    # "triangulation". Reads the candidate that matches best_email and
    # translates its source + pattern + NB result into one short phrase.
    # Exposed to the CSV as the "Email Source" column.
    email_source_detail = _describe_email_source(result)

    return {
        "primary_email": result.best_email or "",
        "scraped_emails": result.evidence_trail.get("discovered_emails", []) or [],
        "constructed_emails": [c.get("email") for c in result.candidate_emails if c.get("email")],
        "contact_name": dm_name,
        "contact_title": dm_title,
        "email_source": email_source_detail,
        "confidence": conf_bucket,
        "synthesis_reasoning": " | ".join(result.best_email_evidence[:3]),
        "synthesizer": "universal_v5",
        # Triangulation-specific fields used by storage.update_business_emails
        "professional_ids_json": _json.dumps(professional_ids),
        "triangulation_pattern": pattern_name,
        "triangulation_confidence": result.best_email_confidence or None,
        "triangulation_method": method,
        "email_safe_to_send": result.safe_to_send,
    }
