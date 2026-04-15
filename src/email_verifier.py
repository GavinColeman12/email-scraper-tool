"""
Email deliverability verification — two tiers:
  1. Free MX-record check (DNS lookup) — catches 80% of dead domains
  2. Optional ZeroBounce API — catches ~95% of dead addresses + confidence score

Strategy: MX first (free). If MX passes but you want higher confidence, fall
through to ZeroBounce only for MX-passing emails (minimizes paid API cost).
"""
import re
import time
import requests

import dns.resolver
import dns.exception

from src.secrets import get_secret

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$")

# Status constants
STATUS_VALID = "valid"
STATUS_INVALID = "invalid"
STATUS_UNKNOWN = "unknown"
STATUS_RISKY = "risky"
STATUS_DISPOSABLE = "disposable"

# MX cache to avoid re-resolving the same domain
_mx_cache = {}


def _domain_of(email: str) -> str:
    m = EMAIL_RE.match(email)
    if not m:
        return ""
    return m.group(1).lower()


def verify_mx(email: str) -> dict:
    """
    Check whether the email's domain has a valid MX record.
    Returns {"status": valid|invalid|unknown, "domain": ..., "has_mx": bool, "reason": ...}
    Takes ~20-100ms per domain (cached).
    """
    result = {
        "email": email,
        "domain": "",
        "has_mx": False,
        "status": STATUS_UNKNOWN,
        "reason": "",
        "method": "mx",
    }

    if not email or "@" not in email:
        result["status"] = STATUS_INVALID
        result["reason"] = "Invalid format"
        return result

    domain = _domain_of(email)
    if not domain:
        result["status"] = STATUS_INVALID
        result["reason"] = "Invalid email format"
        return result
    result["domain"] = domain

    # Check cache first
    if domain in _mx_cache:
        cached = _mx_cache[domain]
        result.update(cached)
        result["email"] = email
        return result

    # DNS MX lookup
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=5)
        has_mx = len(answers) > 0
        result["has_mx"] = has_mx
        if has_mx:
            result["status"] = STATUS_VALID
            result["reason"] = f"Domain has {len(answers)} MX record(s)"
        else:
            result["status"] = STATUS_INVALID
            result["reason"] = "No MX records"
    except dns.resolver.NXDOMAIN:
        result["status"] = STATUS_INVALID
        result["reason"] = "Domain does not exist"
    except dns.resolver.NoAnswer:
        result["status"] = STATUS_INVALID
        result["reason"] = "No MX records"
    except dns.exception.Timeout:
        result["status"] = STATUS_UNKNOWN
        result["reason"] = "DNS timeout"
    except Exception as e:
        result["status"] = STATUS_UNKNOWN
        result["reason"] = f"DNS error: {e}"

    # Cache the result (omit the email-specific field)
    _mx_cache[domain] = {
        k: v for k, v in result.items() if k not in ("email",)
    }
    return result


def verify_zerobounce(email: str) -> dict:
    """
    Use ZeroBounce API to verify an email. Requires ZEROBOUNCE_API_KEY in .env.
    Cost: ~$0.007 per email.
    Returns {"status": valid|invalid|risky|unknown|disposable, ...}.
    """
    result = {
        "email": email,
        "status": STATUS_UNKNOWN,
        "reason": "",
        "method": "zerobounce",
    }

    api_key = get_secret("ZEROBOUNCE_API_KEY")
    if not api_key:
        result["reason"] = "ZEROBOUNCE_API_KEY not set — skipping paid verification"
        return result

    try:
        resp = requests.get(
            "https://api.zerobounce.net/v2/validate",
            params={"api_key": api_key, "email": email},
            timeout=15,
        )
        if resp.status_code != 200:
            result["reason"] = f"ZeroBounce HTTP {resp.status_code}"
            return result
        data = resp.json()
        zb_status = (data.get("status") or "").lower()

        # Map ZeroBounce statuses to ours
        if zb_status == "valid":
            result["status"] = STATUS_VALID
        elif zb_status == "invalid":
            result["status"] = STATUS_INVALID
        elif zb_status in ("do_not_mail", "spamtrap"):
            result["status"] = STATUS_INVALID
        elif zb_status == "abuse":
            result["status"] = STATUS_INVALID
        elif zb_status == "catch-all":
            result["status"] = STATUS_RISKY
        elif zb_status == "unknown":
            result["status"] = STATUS_UNKNOWN
        else:
            result["status"] = STATUS_UNKNOWN

        result["reason"] = data.get("sub_status") or zb_status
        result["zb_data"] = data
    except Exception as e:
        result["reason"] = f"ZeroBounce error: {e}"

    return result


def verify_email(email: str, paid_check: bool = False) -> dict:
    """
    Two-tier verification:
      - Tier 1: free MX check (always runs)
      - Tier 2: optional ZeroBounce (only if paid_check=True AND MX passed)

    Returns combined result with the most trustworthy status.
    """
    # Tier 1: free MX
    mx_result = verify_mx(email)

    # If MX failed, no point paying for ZeroBounce
    if mx_result["status"] == STATUS_INVALID:
        return mx_result

    if not paid_check:
        return mx_result

    # Tier 2: ZeroBounce (only for MX-passing emails)
    zb_result = verify_zerobounce(email)
    # If ZB was skipped (no key), fall back to MX
    if zb_result.get("reason", "").startswith("ZEROBOUNCE_API_KEY not set"):
        return mx_result

    # ZeroBounce is more authoritative when available
    combined = dict(zb_result)
    combined["mx"] = {"has_mx": mx_result.get("has_mx"), "domain": mx_result.get("domain")}
    return combined


def batch_verify(emails: list, paid_check: bool = False,
                 progress_callback=None) -> list:
    """
    Verify a list of emails. Returns a list of result dicts in the same order.
    progress_callback(i, total, email, result) is called after each.
    """
    results = []
    for i, email in enumerate(emails):
        r = verify_email(email, paid_check=paid_check)
        results.append(r)
        if progress_callback:
            progress_callback(i, len(emails), email, r)
        # Light rate-limit to avoid hammering DNS servers
        if i % 20 == 19:
            time.sleep(0.1)
    return results
