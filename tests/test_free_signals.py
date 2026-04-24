"""
Unit tests for the Tier-1 free-signal harvesters:
  - CMS detection + catchall interpretation
  - LinkedIn slug → name parsing
  - WHOIS registrant name extraction (via mocked rdap.org)
  - Copyright footer surname signal
  - Meta author tag harvest
  - Per-run domain pattern cache
  - Multi-year Wayback param plumbing
"""
from unittest.mock import MagicMock, patch

import pytest

from src.cms_detector import detect_cms, catchall_adjustment
from src.free_signals import (
    linkedin_slug_names, meta_author_names,
    footer_lastname_signals,
    cache_domain_pattern, get_domain_pattern, clear_domain_cache,
    whois_registrant_names, _looks_like_personal_name,
)
from src.volume_mode.ranking import Candidate, confidence_tier


# ──────────────────────────────────────────────────────────────────────
# CMS detection
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("html_snippet,expected_cms,expected_catchall", [
    # Squarespace → catchall SUSPECT (usually Google Workspace)
    ('<script src="https://static1.squarespace.com/a.js"></script>',
     "squarespace", "suspect"),
    ('<body class="sqs-block sqs-lightbox">', "squarespace", "suspect"),

    # Wix → catchall REAL
    ('<script src="https://static.wixstatic.com/media/a.jpg"></script>',
     "wix", "real"),
    ('<link href="https://parastorage.com/unpkg/foo.js">', "wix", "real"),

    # Shopify → catchall REAL
    ('<script src="https://cdn.shopify.com/foo.js"></script>',
     "shopify", "real"),

    # Webflow → unknown (could be Google or self-hosted)
    ('<link href="https://assets-global.website-files.com/abc.css">',
     "webflow", "unknown"),

    # WordPress → unknown
    ('<link href="/wp-content/themes/foo/style.css">',
     "wordpress", "unknown"),

    # GoDaddy builder → catchall REAL
    ('<link href="https://img1.wsimg.com/abc.css">',
     "godaddy_websitebuilder", "real"),
])
def test_cms_fingerprint(html_snippet, expected_cms, expected_catchall):
    fp = detect_cms(html_snippet)
    assert fp is not None, f"no fingerprint for {expected_cms}"
    assert fp.cms == expected_cms
    assert fp.catchall_hint == expected_catchall


def test_cms_none_for_custom_site():
    """Site with no fingerprints returns None."""
    fp = detect_cms('<html><body>Plain HTML</body></html>')
    assert fp is None


def test_cms_header_promotes_confidence():
    """X-Wix-* headers should lock Wix detection to 100."""
    fp = detect_cms(
        '<script src="https://static.wixstatic.com/a.js"></script>',
        headers={"X-Wix-Request-Id": "abc"},
    )
    assert fp.cms == "wix"
    assert fp.confidence == 100


def test_catchall_adjustment_directions():
    from src.cms_detector import CMSFingerprint
    # Squarespace → review
    fp = CMSFingerprint("squarespace", 95, "suspect", "google_workspace", [])
    nudge, _ = catchall_adjustment(fp)
    assert nudge == "review"
    # Wix → trust_catchall
    fp = CMSFingerprint("wix", 95, "real", "platform_mailbox", [])
    nudge, _ = catchall_adjustment(fp)
    assert nudge == "trust_catchall"
    # None → keep
    nudge, _ = catchall_adjustment(None)
    assert nudge == "keep"


def test_confidence_tier_cms_review_nudge():
    """NB-catchall + CMS-suspect → volume_review (not volume_scraped)."""
    c = Candidate(email="founder@squarespace-biz.com", bucket="d",
                  pattern="{f}{last}", nb_result="catchall")
    # Default (no CMS info)
    assert confidence_tier(c) == "volume_guess"
    # Keep is also default
    assert confidence_tier(c, cms_catchall_hint="keep") == "volume_guess"
    # Review nudge overrides (squarespace catchall → review)
    assert confidence_tier(c, cms_catchall_hint="review") == "volume_review"

    # Bucket c with catchall + review nudge → also review
    c2 = Candidate(email="founder@squarespace-biz.com", bucket="c",
                   pattern="scraped", nb_result="catchall")
    assert confidence_tier(c2) == "volume_scraped"
    assert confidence_tier(c2, cms_catchall_hint="review") == "volume_review"


# ──────────────────────────────────────────────────────────────────────
# LinkedIn slug → name
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("href,expected_name", [
    ("https://www.linkedin.com/in/paul-s-anderson-09b6936", "Paul S Anderson"),
    ("https://in.linkedin.com/in/foluso-salami", "Foluso Salami"),
    ("https://www.linkedin.com/in/jane-doe-a1b2c3d", "Jane Doe"),
    ("https://www.linkedin.com/in/william-brice-4a5b67c", "William Brice"),
])
def test_linkedin_slug_name(href, expected_name):
    html = f'<a href="{href}">LinkedIn</a>'
    names = linkedin_slug_names(html, "example.com")
    assert len(names) == 1
    assert names[0]["full_name"] == expected_name


def test_linkedin_slug_dedups():
    html = (
        '<a href="https://www.linkedin.com/in/paul-anderson-09b">1</a>'
        '<a href="https://www.linkedin.com/in/paul-anderson-09b">2</a>'
    )
    assert len(linkedin_slug_names(html, "example.com")) == 1


def test_linkedin_slug_rejects_company_slugs():
    """If slug ends in 'Law' / 'LLC' / 'Firm' it's a company, not a person."""
    html = '<a href="https://www.linkedin.com/in/anderson-law">x</a>'
    assert linkedin_slug_names(html, "ex.com") == []


# ──────────────────────────────────────────────────────────────────────
# Footer lastname + meta author
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("html,expected_surnames", [
    ("© 2024 Smith & Jones LLP. All rights reserved.", ["Smith", "Jones"]),
    ("Copyright 2019-2024 Weaver Law Firm", ["Weaver"]),
    ("<div>© 2020 The Law Office of John Buhrman.</div>",
     ["John", "Buhrman"]),
    ("<footer>&copy; 2025 Barbieri Law Firm | PLLC</footer>",
     ["Barbieri"]),
])
def test_footer_lastname(html, expected_surnames):
    found = footer_lastname_signals(html)
    for name in expected_surnames:
        assert name in found, f"missing {name!r} in {found}"


def test_meta_author_tag():
    html = (
        '<html><head>'
        '<meta name="author" content="Paul Anderson">'
        '<meta name="twitter:creator" content="@handle">'
        '</head></html>'
    )
    names = meta_author_names(html)
    # Paul Anderson kept; @handle rejected
    assert len(names) == 1
    assert names[0]["full_name"] == "Paul Anderson"


def test_meta_author_rejects_privacy_strings():
    html = '<meta name="author" content="Domains By Proxy">'
    assert meta_author_names(html) == []


# ──────────────────────────────────────────────────────────────────────
# Per-run domain pattern cache
# ──────────────────────────────────────────────────────────────────────

def test_domain_pattern_cache_roundtrip():
    clear_domain_cache()
    cache_domain_pattern("kazi.law", {"pattern_name": "{f}{last}", "confidence": 95})
    assert get_domain_pattern("kazi.law")["pattern_name"] == "{f}{last}"
    # Case-insensitive
    assert get_domain_pattern("KAZI.LAW")["pattern_name"] == "{f}{last}"
    # Non-existent domain → None
    assert get_domain_pattern("other.com") is None


def test_domain_cache_cleared_between_runs():
    clear_domain_cache()
    cache_domain_pattern("foo.com", {"pattern_name": "first"})
    assert get_domain_pattern("foo.com") is not None
    clear_domain_cache()
    assert get_domain_pattern("foo.com") is None


# ──────────────────────────────────────────────────────────────────────
# WHOIS registrant (rdap.org)
# ──────────────────────────────────────────────────────────────────────

def test_whois_registrant_personal_name():
    mock_cache = MagicMock()
    mock_cache.get.return_value = None
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "entities": [{
            "roles": ["registrant"],
            "vcardArray": ["vcard", [
                ["version", {}, "text", "4.0"],
                ["fn", {}, "text", "Jane Smith"],
            ]],
        }],
    }
    with patch("requests.get", return_value=mock_resp):
        result = whois_registrant_names("example.com", mock_cache)
    assert len(result) == 1
    assert result[0]["full_name"] == "Jane Smith"
    assert result[0]["source"] == "whois"
    assert result[0]["title"] == "Registrant"


def test_whois_registrant_privacy_shield_rejected():
    mock_cache = MagicMock()
    mock_cache.get.return_value = None
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "entities": [{
            "roles": ["registrant"],
            "vcardArray": ["vcard", [
                ["fn", {}, "text", "Domains By Proxy, LLC"],
            ]],
        }],
    }
    with patch("requests.get", return_value=mock_resp):
        result = whois_registrant_names("example.com", mock_cache)
    assert result == []


def test_looks_like_personal_name():
    assert _looks_like_personal_name("Jane Smith")
    assert _looks_like_personal_name("John R Anderson")
    assert not _looks_like_personal_name("Domains By Proxy")
    assert not _looks_like_personal_name("Privacy Contact")
    assert not _looks_like_personal_name("Redacted For Privacy")
    assert not _looks_like_personal_name("Single")  # one token


# ──────────────────────────────────────────────────────────────────────
# Multi-year Wayback — plumbing only (no live fetch)
# ──────────────────────────────────────────────────────────────────────

def test_wayback_accepts_historical_years_param():
    """Verify the function signature accepts `historical_years`. Actual
    fetch behavior requires a live web.archive.org call — validated in
    integration, not unit tests."""
    from src.volume_mode.wayback import fetch_wayback_pages
    import inspect
    sig = inspect.signature(fetch_wayback_pages)
    assert "historical_years" in sig.parameters
    # Default = None (no historical queries, matches old behavior)
    assert sig.parameters["historical_years"].default is None
