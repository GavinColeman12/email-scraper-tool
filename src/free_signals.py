"""
Free-signal harvesters for volume mode.

Scrapes every bit of free data we can pull out of signals we already
fetch — no extra network calls beyond what the existing pipeline
already does. Each function is small, cached where it helps, and
returns OwnerCandidate-compatible dicts so the synthesizer can merge
them with website/linkedin/wayback candidates uniformly.

Signals covered here:

  1. WHOIS registrant name  — free via rdap.org, already in triangulation
  2. LinkedIn slug → name   — from homepage <a> hrefs (no LI fetch)
  3. Copyright footer       — "© 2024 Smith & Jones" → lastname signals
  4. Meta tag author        — <meta name="author"|"twitter:creator">
  5. Multi-year Wayback     — fetch 1yr/3yr/5yr-old snapshots, not just recent

Each returns a (candidates, extra_emails) tuple with the standard
OwnerCandidate dataclass shape (via dict).
"""
from __future__ import annotations

import re
from typing import Optional


# ──────────────────────────────────────────────────────────────────────
# WHOIS registrant name (via rdap.org — same API triangulation uses)
# ──────────────────────────────────────────────────────────────────────

def whois_registrant_names(domain: str, cache) -> list[dict]:
    """
    Return OwnerCandidate dicts (ready to extend into the candidate list)
    for any registrant / administrative / technical contacts found via
    WHOIS. rdap.org is free. Cached 90 days.

    Shape matches OwnerCandidate: full_name/first_name/last_name/title/
    source/source_url/confidence.
    """
    if not domain:
        return []

    cached = cache.get("whois_candidates", domain)
    if cached is not None:
        return list(cached)

    import requests
    try:
        resp = requests.get(
            f"https://rdap.org/domain/{domain}",
            timeout=10, headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            cache.set("whois_candidates", [], domain)
            return []
        data = resp.json()
    except Exception:
        return []

    out: list[dict] = []
    for entity in data.get("entities", []):
        roles = [r.lower() for r in entity.get("roles", [])]
        if not any(r in roles for r in ("registrant", "administrative", "technical")):
            continue
        vcard = entity.get("vcardArray", [])
        if len(vcard) < 2:
            continue
        for item in vcard[1]:
            if len(item) >= 4 and item[0] == "fn" and item[3]:
                full = str(item[3]).strip()
                if not _looks_like_personal_name(full):
                    continue
                parts = full.split(None, 1)
                first = parts[0] if parts else ""
                last = parts[1] if len(parts) > 1 else ""
                # Role label for credential-gate — registrants are
                # almost always owners/founders on small biz domains.
                title = "Registrant" if "registrant" in roles else roles[0].capitalize()
                out.append({
                    "full_name": full,
                    "first_name": first,
                    "last_name": last,
                    "title": title,
                    "source": "whois",
                    "source_url": f"rdap://{domain}",
                    "confidence": 65,
                    "raw_snippet": "",
                })
                break

    cache.set("whois_candidates", out, domain)
    return out


def _looks_like_personal_name(s: str) -> bool:
    """Reject WHOIS privacy shields and corporate registrant strings."""
    s_low = s.lower()
    privacy_markers = (
        "privacy", "whoisguard", "domains by proxy", "redacted",
        "data protected", "gdpr", "proxy contact", "private",
        "contact privacy", "domain admin",
    )
    if any(m in s_low for m in privacy_markers):
        return False
    # Two tokens, both letters = plausible person name
    tokens = s.split()
    if len(tokens) < 2:
        return False
    # Allow single-letter tokens (middle initials like "John R Anderson")
    if not all(re.match(r"^[A-Za-z][A-Za-z\.\-']*$", t) for t in tokens[:2]):
        return False
    return True


# ──────────────────────────────────────────────────────────────────────
# LinkedIn slug → name (no LI fetch — just URL parsing)
# ──────────────────────────────────────────────────────────────────────

# LinkedIn profile URLs look like:
#   https://www.linkedin.com/in/paul-s-anderson-09b6936
#   https://www.linkedin.com/in/jsmith
#   https://in.linkedin.com/in/foluso-salami
# The slug after /in/ is hyphen-separated name tokens, often followed
# by a random hash. We extract the name tokens and stop at the hash.

_LINKEDIN_URL_RE = re.compile(
    r"""https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/
        ([a-z][a-z0-9\-]{2,80})        # slug: letters, digits, hyphens
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Hash suffix detector — 5+ alphanumerics at slug end that MUST contain
# a digit (pure-letter tokens are last names, not hashes). LinkedIn's
# random hash is always something like "09b6936" / "a1b2c3d".
_SLUG_HASH_RE = re.compile(r"-([a-z0-9]*\d[a-z0-9]*)$", re.IGNORECASE)


def linkedin_slug_names(html: str, domain: str) -> list[dict]:
    """
    Find LinkedIn profile URLs embedded in the site and extract names
    from the slug portion. Zero network calls — just URL parsing.

    Example: linkedin.com/in/paul-s-anderson-09b6936 → "Paul S Anderson"
    """
    if not html:
        return []

    seen_slugs = set()
    out: list[dict] = []

    for m in _LINKEDIN_URL_RE.finditer(html):
        slug = m.group(1).lower()
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        # Strip trailing hash suffix (e.g. "-09b6936")
        core = _SLUG_HASH_RE.sub("", slug)
        tokens = [t for t in core.split("-") if t]
        # Need at least first + last
        if len(tokens) < 2:
            continue
        # Single-letter tokens are initials — keep them but cap to 3
        # total name parts for "John R Anderson" style.
        tokens = tokens[:3]
        full = " ".join(t.capitalize() for t in tokens)
        first = tokens[0].capitalize()
        last = tokens[-1].capitalize()

        # Sanity: reject slugs that look like company/brand slugs rather
        # than person names (common on linkedin.com/company/... but we
        # already filtered by /in/ path; belt-and-suspenders)
        if last.lower() in ("law", "llc", "inc", "firm", "group"):
            continue

        out.append({
            "full_name": full,
            "first_name": first,
            "last_name": last,
            "title": "",
            "source": "linkedin_slug",
            "source_url": m.group(0),
            "confidence": 55,
            "raw_snippet": "",
        })

    return out


# ──────────────────────────────────────────────────────────────────────
# Copyright footer + meta-tag author names
# ──────────────────────────────────────────────────────────────────────

# "© 2024 Smith & Jones LLP" / "Copyright 2019-2024 The Law Office of John Doe"
# Tolerates HTML entities (&amp;), punctuation terminators (. or ,), and
# common trailing phrases ("All rights reserved", "</div>").
_COPYRIGHT_RE = re.compile(
    r"(?:©|&copy;|copyright)\s*\d{4}(?:\s*[\-–—]\s*\d{4})?\s*"
    r"(?:by\s+)?"
    r"([A-Z][A-Za-z\.\-'&,;\s]{3,120}?)"
    r"(?=[.|<]|all\s+rights|\s*$)",
    re.IGNORECASE,
)


def footer_lastname_signals(html: str) -> list[str]:
    """
    Extract last-name tokens from copyright footers for pattern
    triangulation evidence. Returns a list of capitalized lastname
    strings. These aren't candidates (no first name), just evidence
    that the footer namedrops these surnames.

    Example: "© 2024 Smith & Jones LLP" → ["Smith", "Jones"]
    """
    if not html:
        return []
    # Look in the last 5KB — footers live at the bottom. Also catches
    # it if the footer HTML wraps around to the "closing" of body.
    tail = html[-5000:] if len(html) > 5000 else html
    found: list[str] = []
    for m in _COPYRIGHT_RE.finditer(tail):
        chunk = m.group(1)
        # Decode HTML entities (&amp; → &) so tokens split correctly
        chunk = chunk.replace("&amp;", "&").replace("&#38;", "&")
        # Strip filler tokens that aren't surnames
        cleaned = re.sub(
            r"\b(?:the|law|office|offices|firm|llc|inc|pllc|pc|pllc|llp|"
            r"group|associates|company|co|corp|corporation|ltd|limited|"
            r"all|rights|reserved|attorneys|at|of|and|amp)\b",
            " ", chunk, flags=re.IGNORECASE,
        )
        for token in re.findall(r"\b[A-Z][a-zA-Z\-']{2,}\b", cleaned):
            if token.lower() in {"the", "all", "rights", "reserved"}:
                continue
            found.append(token)
    # Dedup while preserving order
    seen = set()
    out = []
    for t in found:
        if t.lower() not in seen:
            seen.add(t.lower())
            out.append(t)
    return out[:10]  # cap — footers can over-match on multi-firm sites


_META_AUTHOR_RES = [
    re.compile(
        r'<meta[^>]+name=["\'](?:author|twitter:creator)["\'][^>]+'
        r'content=["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(
        r'<meta[^>]+property=["\'](?:article:author|og:author)["\'][^>]+'
        r'content=["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+'
        r'name=["\'](?:author|twitter:creator)["\']', re.IGNORECASE),
]


def meta_author_names(html: str) -> list[dict]:
    """
    Extract <meta name="author"> / twitter:creator / og:author. These
    are often the site owner's name filled in by WordPress / Wix /
    Squarespace theme defaults.
    """
    if not html:
        return []
    seen = set()
    out: list[dict] = []
    for pattern in _META_AUTHOR_RES:
        for m in pattern.finditer(html):
            raw = m.group(1).strip()
            if not raw or raw.startswith("@"):
                # Twitter creator often leads with @handle; we can't
                # resolve those to a person without LI fetch. Skip.
                if raw.startswith("@"):
                    continue
                continue
            if not _looks_like_personal_name(raw):
                continue
            if raw.lower() in seen:
                continue
            seen.add(raw.lower())
            parts = raw.split(None, 1)
            out.append({
                "full_name": raw,
                "first_name": parts[0] if parts else "",
                "last_name": parts[1] if len(parts) > 1 else "",
                "title": "",
                "source": "meta_author",
                "source_url": "",
                "confidence": 50,
                "raw_snippet": "",
            })
    return out


# ──────────────────────────────────────────────────────────────────────
# Per-run domain pattern cache
# ──────────────────────────────────────────────────────────────────────

# Simple process-global dict — bulk-scrape runs typically all share one
# Python process. Cleared at the start of each run via clear_domain_cache().
# Keyed by domain → (pattern_name, confidence, method)
_DOMAIN_PATTERN_CACHE: dict[str, dict] = {}


def cache_domain_pattern(domain: str, pattern_dict: dict) -> None:
    """Stash a triangulated pattern per-domain so later biz at the same
    domain inherit instead of re-computing."""
    if not domain or not pattern_dict:
        return
    _DOMAIN_PATTERN_CACHE[domain.lower()] = dict(pattern_dict)


def get_domain_pattern(domain: str) -> Optional[dict]:
    """Look up a cached pattern for a domain (chain restaurants /
    multi-location firms reuse the same email scheme)."""
    if not domain:
        return None
    return _DOMAIN_PATTERN_CACHE.get(domain.lower())


def clear_domain_cache() -> None:
    """Reset at the start of a new bulk-scrape run."""
    _DOMAIN_PATTERN_CACHE.clear()
