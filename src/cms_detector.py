"""
CMS / website-builder detection with email-provider inference.

Why this matters for the scraper:
  The NeverBounce verdict "catchall" means "the domain accepts mail to
  ANY local part — we can't confirm the specific mailbox exists". That
  verdict is interpreted differently per hosting platform:

    - Squarespace sites default to Google Workspace → rarely catch-all.
      A "catchall" verdict on a Squarespace site is suspicious and
      likely means the specific mailbox exists but NB hit a rate limit
      or SPF/DMARC wall. Treat as volume_review, not volume_empty.

    - Wix / Weebly / GoDaddy Website Builder → platform-managed mailbox
      with aggressive catch-all defaults. A "catchall" verdict here is
      usually real; trust it as low-confidence.

    - Shopify → Shopify Mail is always catch-all. Strong downgrade.

    - WordPress / Webflow → varies (depends on the site owner's email
      host choice). No strong signal from CMS alone.

  We detect the CMS from signals already in the homepage HTML we've
  fetched (no extra network calls), then return a {cms, catchall_hint,
  provider_hint} tuple the ranking logic can use.

Free — runs on HTML we already have in the caching pipeline.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class CMSFingerprint:
    cms: str                       # "wordpress" | "squarespace" | "wix" | ...
    confidence: int                # 0-100
    # How to interpret NB "catchall" verdicts on this CMS:
    #   "real"      — platform genuinely catch-alls everything (Wix,
    #                  Shopify); trust NB verdict
    #   "suspect"   — platform usually doesn't catch-all (Squarespace,
    #                  Webflow w/ Google Workspace); NB may be wrong
    #   "unknown"   — no signal either way (WordPress, custom)
    catchall_hint: str
    # Inferred email provider — informational, can inform follow-up:
    #   "google_workspace" | "m365" | "platform_mailbox" |
    #   "self_hosted" | "unknown"
    provider_hint: str
    # Evidence that fired — for the decision log
    evidence: list[str]


# ── Fingerprint rules — ordered by specificity ────────────────────────
#
# Each rule is (cms_name, confidence_if_match, catchall_hint,
#               provider_hint, [patterns], tag="cms_signature")
# Patterns are regex strings checked (case-insensitively) against the
# homepage HTML. Multiple matches increase confidence.

_RULES = [
    ("shopify", 95, "real", "platform_mailbox", [
        r"cdn\.shopify\.com",
        r"Shopify\.theme",
        r"shopify-features",
        r"myshopify\.com",
    ]),
    ("wix", 95, "real", "platform_mailbox", [
        r"static\.wixstatic\.com",
        r"X-Wix-",
        r"wix\.com/website",
        r"parastorage\.com",
        r"_wixCIDX",
    ]),
    ("squarespace", 95, "suspect", "google_workspace", [
        r"static1\.squarespace\.com",
        r"squarespace\.com",
        r"assets\.squarespace\.com",
        r"class=\"sqs-",
        r"Static\.SQUARESPACE_CONTEXT",
    ]),
    ("webflow", 90, "unknown", "unknown", [
        r"webflow\.com",
        r"assets-global\.website-files\.com",
        r"data-wf-page",
        r"webflow\.js",
    ]),
    ("weebly", 90, "real", "platform_mailbox", [
        r"weebly\.com",
        r"cdn2\.editmysite\.com",
        r"__wc_cdn",
        r"wsite_",
    ]),
    ("godaddy_websitebuilder", 90, "real", "platform_mailbox", [
        r"img1\.wsimg\.com",
        r"gd-websites-v",
        r"websitebuilder\.godaddy",
        r"dpbnri2zg3lc\.cloudfront\.net",
    ]),
    ("duda", 90, "real", "platform_mailbox", [
        r"multiscreensite\.com",
        r"dudamobile\.com",
        r"_dm_",
    ]),
    # WordPress — broad fingerprint, runs AFTER more-specific CMSes
    # (Shopify / Squarespace sometimes embed wp-* strings as decoy).
    ("wordpress", 85, "unknown", "unknown", [
        r"wp-content/",
        r"wp-includes/",
        r"name=\"generator\"[^>]*WordPress",
        r"wp-json/",
        r"wordpress_[a-z_]*=",  # cookie name
    ]),
]


def detect_cms(html: str, headers: Optional[dict] = None) -> Optional[CMSFingerprint]:
    """
    Run all fingerprint rules and return the highest-confidence match,
    or None if nothing fingerprinted (likely custom / unknown).

    `headers` is optional — if supplied, we also check response headers
    (X-Wix-*, X-Powered-By, Server) which are extra-strong signals.
    """
    if not html:
        return None
    lower = html.lower()

    best: Optional[CMSFingerprint] = None
    best_score = 0

    for cms, base_conf, catchall, provider, patterns in _RULES:
        matched_patterns = []
        for p in patterns:
            if re.search(p, html, re.IGNORECASE):
                matched_patterns.append(p)
        if not matched_patterns:
            continue

        # Confidence scales with number of matches (more = more certain,
        # less likely to be a decoy/reference in blog content).
        score = min(base_conf + 3 * (len(matched_patterns) - 1), 100)

        # Header signals — high-trust
        if headers:
            hdr_blob = " ".join(f"{k}: {v}" for k, v in headers.items()).lower()
            if cms == "wix" and "x-wix-" in hdr_blob:
                score = 100
                matched_patterns.append("header:X-Wix-*")
            if cms == "wordpress" and "wordpress" in hdr_blob:
                score = min(score + 5, 100)
                matched_patterns.append("header:wordpress")
            if cms == "shopify" and "shopify" in hdr_blob:
                score = 100
                matched_patterns.append("header:shopify")

        if score > best_score:
            best_score = score
            best = CMSFingerprint(
                cms=cms,
                confidence=score,
                catchall_hint=catchall,
                provider_hint=provider,
                evidence=matched_patterns[:5],
            )

    # Generator meta tag — parse separately for extra signals any CMS
    # might use ("Joomla", "Drupal", "HubSpot CMS", etc.)
    if best is None:
        m = re.search(r'<meta[^>]+name=[\'"]generator[\'"][^>]+content=[\'"]([^\'"]+)',
                      html, re.IGNORECASE)
        if m:
            gen = m.group(1).lower()
            for name in ("hubspot", "joomla", "drupal", "ghost", "silverstripe",
                          "sitecore", "contentful", "magento"):
                if name in gen:
                    return CMSFingerprint(
                        cms=name, confidence=80,
                        catchall_hint="unknown", provider_hint="unknown",
                        evidence=[f"meta generator: {gen[:50]}"],
                    )

    return best


def catchall_adjustment(cms_fp: Optional[CMSFingerprint]) -> tuple[str, str]:
    """
    Helper for the ranking layer. Given a CMS fingerprint, return
    (verdict_nudge, rationale) for how to re-interpret an NB "catchall"
    verdict.

      verdict_nudge ∈ {"trust_catchall", "review", "keep"}

    "trust_catchall"  → the catchall IS real; keep the NB verdict as-is
                        and don't promote the row above guess tier.
    "review"          → the catchall is suspicious; move the row into
                        volume_review so the operator sees it before
                        anything goes out.
    "keep"            → no signal; preserve default behavior.
    """
    if cms_fp is None:
        return ("keep", "no CMS fingerprinted")
    if cms_fp.catchall_hint == "real":
        return ("trust_catchall",
                f"{cms_fp.cms} typically catch-alls; trust the NB verdict")
    if cms_fp.catchall_hint == "suspect":
        return ("review",
                f"{cms_fp.cms} usually isn't catch-all — NB may be wrong; "
                "needs human review before send")
    return ("keep", f"{cms_fp.cms}: no catchall signal")
