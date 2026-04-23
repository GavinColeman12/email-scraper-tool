"""
Block building emails against link-shortener / redirect domains.

Problem seen in the wild (search 44):
  Faulkner Law Firm's Google Maps entry had website =
  https://gtmaps.top/business/faulkner-law-firm-llc-hcm0sa
  which is a third-party Google-Maps scraper/redirector, not the
  firm's real domain. Volume mode happily generated
  sfaulkner@gtmaps.top patterns against it.

Rule: if the business website's registrable domain is in this list,
treat the domain as UNKNOWN (don't build bucket-D/E patterns, don't
try SMTP). Bucket-A scraped emails are still fine if they come from
somewhere else.

Kept narrow — these are domains known to NOT be operated by the
businesses whose info they display.
"""
from __future__ import annotations

from urllib.parse import urlparse


REDIRECT_DOMAINS = frozenset({
    # Google Maps / Places scrapers & redirectors
    "gtmaps.top", "mapsdirection.com", "mapsquest.info",
    "maps.app.goo.gl", "g.page", "goo.gl",
    # General URL shorteners
    "bit.ly", "tinyurl.com", "t.co", "ow.ly", "buff.ly",
    "is.gd", "v.gd", "rebrand.ly", "rb.gy", "cutt.ly",
    "lnkd.in", "fb.me", "ift.tt",
    # Business-listing aggregators (contact info routes through them,
    # not the business itself)
    "yelp.com", "yellowpages.com", "bbb.org", "angi.com",
    "thumbtack.com", "nextdoor.com",
    # Booking / reservation platforms (contact via the platform, not
    # the business email)
    "opentable.com", "resy.com", "tock.com",
    "bookingbug.com", "vagaro.com", "schedulicity.com",
})


def extract_domain(url: str) -> str:
    """Extract the host portion of a URL (lowercased, stripped of
    leading 'www.'). Empty string if parsing fails."""
    if not url:
        return ""
    if "://" not in url:
        url = "http://" + url
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return ""
    host = host.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def is_redirect_domain(url_or_domain: str) -> bool:
    """
    True if the URL points at a known redirector / shortener / listing
    aggregator — meaning the business's real email domain is elsewhere
    and we must NOT synthesize emails against this host.
    """
    if not url_or_domain:
        return False
    # Accept either full URL or bare domain
    host = extract_domain(url_or_domain) if ("://" in url_or_domain or "/" in url_or_domain) else url_or_domain.lower()
    if not host:
        return False
    if host in REDIRECT_DOMAINS:
        return True
    # Check parent domain for subdomains (e.g. "foo.maps.app.goo.gl")
    parts = host.split(".")
    for i in range(len(parts) - 1):
        parent = ".".join(parts[i:])
        if parent in REDIRECT_DOMAINS:
            return True
    return False
