"""
Advanced email extraction techniques beyond plain regex + mailto.

These handle the cases where businesses HIDE their emails to avoid scrapers:
  1. Cloudflare email protection (data-cfemail attribute)
  2. Obfuscated patterns — 'name [at] domain [dot] com', '(at)', '{at}'
  3. HTML entity encoded — &#x6a;ane&#64;example.com
  4. JavaScript assembly — emailName + '@' + emailDomain
  5. JSON-LD structured data — Organization.contactPoint, ContactPage
  6. Meta tags — <meta name="contact" content="...">
  7. Footer reveal sections — common in WordPress themes
"""
import re
from bs4 import BeautifulSoup


# ── Cloudflare email decoding ─────────────────────────────────────────

def decode_cloudflare_email(encoded: str) -> str:
    """
    Decode a Cloudflare-protected email. These look like:
        <a href="/cdn-cgi/l/email-protection#abc123..." data-cfemail="abc123...">

    The encoding: first byte is the XOR key; remaining bytes XOR with it
    to produce the email.
    """
    try:
        key = int(encoded[:2], 16)
        decoded = ""
        for i in range(2, len(encoded), 2):
            decoded += chr(int(encoded[i:i + 2], 16) ^ key)
        return decoded
    except Exception:
        return ""


def extract_cloudflare_emails(html: str) -> list:
    """Find all Cloudflare-obfuscated emails in the page."""
    emails = []
    soup = BeautifulSoup(html, "html.parser")
    for el in soup.find_all(attrs={"data-cfemail": True}):
        decoded = decode_cloudflare_email(el.get("data-cfemail", ""))
        if decoded and "@" in decoded:
            emails.append(decoded.lower())
    # Also match the pattern in raw HTML as a fallback
    for match in re.finditer(r'data-cfemail="([a-f0-9]+)"', html, re.IGNORECASE):
        decoded = decode_cloudflare_email(match.group(1))
        if decoded and "@" in decoded and decoded.lower() not in emails:
            emails.append(decoded.lower())
    return emails


# ── Obfuscated patterns ───────────────────────────────────────────────

# Matches: "user [at] example [dot] com", "user (at) example (dot) com",
# "user{at}example{dot}com", "user AT example DOT com"
_OBFUSCATED_RE = re.compile(
    r"""([a-zA-Z0-9._%+-]+)                 # local part
        \s*[\[\(\{]?\s*(?:at|@)\s*[\]\)\}]?\s*   # @ obfuscation
        ([a-zA-Z0-9.-]+)                     # domain before TLD
        \s*[\[\(\{]?\s*(?:dot|\.)\s*[\]\)\}]?\s*  # . obfuscation
        ([a-zA-Z]{2,})                       # TLD
    """,
    re.IGNORECASE | re.VERBOSE,
)


def extract_obfuscated_emails(text: str) -> list:
    """Pull emails hidden with [at] / [dot] / (at) / {at} patterns."""
    emails = []
    for match in _OBFUSCATED_RE.finditer(text):
        local, domain_pre, tld = match.groups()
        # Skip if 'at' or 'dot' landed inside a real word
        if len(local) < 2 or len(domain_pre) < 2:
            continue
        email = f"{local}@{domain_pre}.{tld}".lower()
        emails.append(email)
    return emails


# ── HTML entity decoding ──────────────────────────────────────────────

def decode_html_entities(html: str) -> str:
    """Decode numeric HTML entities like &#x6a; and &#106; to characters."""
    # Hex entities
    html = re.sub(
        r"&#x([0-9a-fA-F]+);",
        lambda m: chr(int(m.group(1), 16)),
        html,
    )
    # Decimal entities
    html = re.sub(
        r"&#(\d+);",
        lambda m: chr(int(m.group(1))),
        html,
    )
    return html


# ── JSON-LD contactPoint extraction ───────────────────────────────────

def extract_jsonld_emails(html: str) -> list:
    """
    Parse JSON-LD structured data for Organization.contactPoint.email,
    ContactPage.email, Person.email, LocalBusiness.email.
    """
    import json
    emails = []
    soup = BeautifulSoup(html, "html.parser")

    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.text or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            _walk_jsonld_for_emails(item, emails)

    return list(dict.fromkeys(emails))  # dedupe preserving order


def _walk_jsonld_for_emails(node, out_list):
    if isinstance(node, dict):
        for k, v in node.items():
            if k.lower() == "email" and isinstance(v, str):
                clean = v.replace("mailto:", "").strip().lower()
                if "@" in clean:
                    out_list.append(clean)
            elif isinstance(v, (dict, list)):
                _walk_jsonld_for_emails(v, out_list)
    elif isinstance(node, list):
        for item in node:
            _walk_jsonld_for_emails(item, out_list)


# ── Meta tag + rel-author extraction ──────────────────────────────────

def extract_meta_emails(html: str) -> list:
    """
    Check meta tags and link rel=author for contact info.
    Some sites use <meta name="contact" content="x@y.com">.
    """
    emails = []
    soup = BeautifulSoup(html, "html.parser")

    for meta in soup.find_all("meta"):
        for attr in ("content", "value"):
            v = meta.get(attr, "")
            if v and "@" in v and "." in v:
                match = re.search(
                    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", v)
                if match:
                    emails.append(match.group(0).lower())

    return list(dict.fromkeys(emails))


# ── JavaScript assembly detection ─────────────────────────────────────

_JS_EMAIL_PATTERNS = [
    # 'user' + '@' + 'domain.com'
    re.compile(
        r"['\"]([a-zA-Z0-9._%+-]+)['\"]"
        r"\s*\+\s*['\"]@['\"]"
        r"\s*\+\s*['\"]([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})['\"]"
    ),
    # emailUser = 'user'; emailDomain = 'domain.com';
    re.compile(
        r"emailUser\s*=\s*['\"]([a-zA-Z0-9._%+-]+)['\"][^;]*?"
        r"emailDomain\s*=\s*['\"]([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})['\"]",
        re.DOTALL | re.IGNORECASE,
    ),
]


def extract_js_assembled_emails(html: str) -> list:
    """Find emails assembled from JavaScript string concatenation."""
    emails = []
    for pattern in _JS_EMAIL_PATTERNS:
        for match in pattern.finditer(html):
            local, domain = match.groups()
            emails.append(f"{local}@{domain}".lower())
    return list(dict.fromkeys(emails))


# ── Attribute extractor (mailto hrefs, aria-labels, data-email) ───────

def extract_attribute_emails(html: str) -> list:
    """
    Extract emails from HTML attributes commonly used for contact info
    but easy to miss in plain-text regex:
      - <a href="mailto:foo@bar.com">
      - <a aria-label="Email John at foo@bar.com">
      - <button data-email="foo@bar.com">
      - <span data-mail="foo@bar.com">
      - <input name="email" value="foo@bar.com"> (admin leaks)
    Some templates hide these behind JS obfuscation that the regex
    pass over rendered text misses entirely.
    """
    if not html:
        return []
    emails = []
    soup = BeautifulSoup(html, "html.parser")
    email_re = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

    # mailto: hrefs — most common hidden source
    for a in soup.find_all("a", href=True):
        href = a["href"] or ""
        if href.lower().startswith("mailto:"):
            body = href[7:].split("?", 1)[0]  # strip "?subject=..." params
            for e in email_re.findall(body):
                emails.append(e.lower())

    # aria-label, title, alt — accessibility attrs often include emails
    for tag in soup.find_all(True):
        for attr in ("aria-label", "title", "alt", "value"):
            v = tag.get(attr)
            if not v:
                continue
            for e in email_re.findall(str(v)):
                emails.append(e.lower())

    # data-* attributes: data-email, data-mail, data-contact, data-cfemail decoded
    for tag in soup.find_all(True):
        attrs = tag.attrs or {}
        for k, v in attrs.items():
            if not isinstance(k, str) or not k.startswith("data-"):
                continue
            if not v:
                continue
            for e in email_re.findall(str(v)):
                emails.append(e.lower())

    return list(dict.fromkeys(emails))  # dedupe preserving order


# ── Master extractor ──────────────────────────────────────────────────

def extract_all_hidden_emails(html: str) -> dict:
    """
    Run all hidden-email extractors on a page. Returns a dict keyed by
    source so the caller can weight them differently.

    {
        "cloudflare": [...],
        "obfuscated": [...],
        "html_entities": [...],
        "jsonld": [...],
        "meta": [...],
        "js_assembled": [...],
        "attributes": [...],
    }
    """
    if not html:
        return {k: [] for k in ("cloudflare", "obfuscated", "html_entities",
                                 "jsonld", "meta", "js_assembled", "attributes")}

    # Decode HTML entities first so they show up in subsequent passes
    decoded_html = decode_html_entities(html)

    return {
        "cloudflare": extract_cloudflare_emails(html),
        "obfuscated": extract_obfuscated_emails(decoded_html),
        "html_entities": _extract_entity_revealed_emails(html, decoded_html),
        "jsonld": extract_jsonld_emails(html),
        "meta": extract_meta_emails(html),
        "js_assembled": extract_js_assembled_emails(html),
        "attributes": extract_attribute_emails(html),
    }


def _extract_entity_revealed_emails(original: str, decoded: str) -> list:
    """Emails that only appeared after HTML entity decoding."""
    email_re = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
    original_set = set(m.group(0).lower() for m in email_re.finditer(original))
    decoded_matches = set(m.group(0).lower() for m in email_re.finditer(decoded))
    return list(decoded_matches - original_set)
