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

# Catch-all detection cache — domains where MX accepts any mailbox
# (makes all constructed emails unreliable). ZeroBounce catches this but
# it's paid; we can estimate by checking if the MX server accepts a
# random prefix at the SMTP layer. For now we just flag known catch-all
# providers.
KNOWN_CATCHALL_PROVIDERS = {
    # Shared hosting / forwarders that often catch-all
    "privateemail.com", "mail.privateemail.com",
    "fwd.bluehost.com", "bluehost.com",
}


def is_known_catchall_mx(mx_host: str) -> bool:
    """Return True if the MX host is a known catch-all provider."""
    host = (mx_host or "").lower().rstrip(".")
    return any(host.endswith(p) for p in KNOWN_CATCHALL_PROVIDERS)


# Disposable email domains — if an email matches these, it's throwaway
DISPOSABLE_DOMAINS = frozenset({
    "10minutemail.com", "guerrillamail.com", "mailinator.com", "tempmail.com",
    "throwaway.email", "trashmail.com", "yopmail.com", "maildrop.cc",
    "sharklasers.com", "guerrillamailblock.com", "emailondeck.com",
    "getnada.com", "fakemailgenerator.com", "temp-mail.org", "dispostable.com",
    "mohmal.com", "tempail.com", "tempmailaddress.com", "jetable.org",
    "spam4.me", "trbvm.com", "mailnesia.com", "mintemail.com",
    "inboxbear.com", "spamgourmet.com", "mytemp.email", "burnermail.io",
})


def is_disposable(email: str) -> bool:
    domain = _domain_of(email)
    if not domain:
        return False
    return domain in DISPOSABLE_DOMAINS or any(
        domain.endswith("." + d) for d in DISPOSABLE_DOMAINS
    )


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


# ── SMTP RCPT TO verification (free, catches bad mailboxes) ──────────

_smtp_cache = {}  # domain -> {"is_catchall": bool, "mx_host": str}


def _get_mx_host(domain: str) -> str:
    """Resolve the primary MX host for a domain."""
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=5)
        # Pick lowest-preference MX (primary)
        best = min(answers, key=lambda r: r.preference)
        return str(best.exchange).rstrip(".")
    except Exception:
        return ""


def verify_smtp(email: str, timeout: int = 10) -> dict:
    """
    Connect to the recipient's mail server and check if the specific
    mailbox exists using SMTP RCPT TO. This is FREE and catches exactly
    the 'address not found' bounces that MX-only verification misses.

    Returns {"status": valid|invalid|unknown, "method": "smtp", ...}

    Caveats:
    - Some servers (Google Workspace) always accept RCPT TO (catch-all)
    - Some servers block this probe; we return 'unknown' in that case
    - Takes 3-8 seconds per check due to SMTP handshake
    """
    import smtplib
    import socket

    result = {
        "email": email,
        "status": STATUS_UNKNOWN,
        "reason": "",
        "method": "smtp",
        "is_catchall": False,
    }

    domain = _domain_of(email)
    if not domain:
        result["status"] = STATUS_INVALID
        result["reason"] = "Invalid email format"
        return result

    # Get MX host
    mx_host = _get_mx_host(domain)
    if not mx_host:
        result["status"] = STATUS_INVALID
        result["reason"] = "No MX host found"
        return result

    try:
        # Connect to SMTP server
        smtp = smtplib.SMTP(timeout=timeout)
        smtp.connect(mx_host, 25)
        smtp.helo("verify.local")

        # First check if domain is catch-all by probing a random address
        if domain not in _smtp_cache:
            import uuid
            random_addr = f"verify-{uuid.uuid4().hex[:8]}@{domain}"
            code_random, _ = smtp.rcpt(random_addr)
            is_catchall = (code_random == 250)
            _smtp_cache[domain] = {"is_catchall": is_catchall, "mx_host": mx_host}
        else:
            is_catchall = _smtp_cache[domain]["is_catchall"]

        if is_catchall:
            result["status"] = STATUS_RISKY
            result["reason"] = "Domain is catch-all (accepts any address)"
            result["is_catchall"] = True
            smtp.quit()
            return result

        # Probe the actual email address
        code, msg = smtp.rcpt(email)
        smtp.quit()

        if code == 250:
            result["status"] = STATUS_VALID
            result["reason"] = "Mailbox exists (SMTP verified)"
        elif code == 550 or code == 551 or code == 553:
            result["status"] = STATUS_INVALID
            result["reason"] = f"Mailbox does not exist (SMTP {code})"
        elif code == 452 or code == 421:
            result["status"] = STATUS_UNKNOWN
            result["reason"] = f"Server busy/rate-limited (SMTP {code})"
        else:
            result["status"] = STATUS_UNKNOWN
            result["reason"] = f"SMTP response {code}"

    except smtplib.SMTPServerDisconnected:
        result["status"] = STATUS_UNKNOWN
        result["reason"] = "Server disconnected (may block verification)"
    except smtplib.SMTPConnectError:
        result["status"] = STATUS_UNKNOWN
        result["reason"] = "Could not connect to mail server"
    except socket.timeout:
        result["status"] = STATUS_UNKNOWN
        result["reason"] = "SMTP timeout"
    except Exception as e:
        result["status"] = STATUS_UNKNOWN
        result["reason"] = f"SMTP error: {type(e).__name__}"

    return result


def verify_smtp_patterns(first: str, last: str, domain: str,
                          timeout: int = 10) -> dict:
    """
    Try multiple email patterns and return the first one that SMTP-verifies
    as valid. If none verify, return the most common pattern.

    This is what catches the bounces: instead of guessing 'amy.morgan@',
    we try 'amy@', 'amy.morgan@', 'amorgan@', 'a.morgan@', 'morgan@'
    and keep the one the mail server accepts.

    Returns {"email": "winning@pattern", "status": ..., "patterns_tried": int}
    """
    first = (first or "").lower().strip()
    last = (last or "").lower().strip()

    if not first or not domain:
        return {"email": "", "status": STATUS_UNKNOWN, "patterns_tried": 0}

    # Generate candidate patterns (most common first)
    candidates = []
    if last:
        candidates = [
            f"{first}@{domain}",
            f"{first}.{last}@{domain}",
            f"{first[0]}{last}@{domain}",
            f"{first}{last}@{domain}",
            f"{first[0]}.{last}@{domain}",
            f"{last}@{domain}",
        ]
    else:
        candidates = [f"{first}@{domain}"]

    # Dedupe preserving order
    seen = set()
    unique = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    best = None
    for email in unique:
        result = verify_smtp(email, timeout=timeout)
        if result["status"] == STATUS_VALID:
            return {
                "email": email,
                "status": STATUS_VALID,
                "reason": result["reason"],
                "patterns_tried": unique.index(email) + 1,
                "method": "smtp_pattern",
            }
        if result.get("is_catchall"):
            # Catch-all domain — can't verify, return first pattern
            return {
                "email": unique[0],
                "status": STATUS_RISKY,
                "reason": "Domain is catch-all — cannot verify specific mailbox",
                "patterns_tried": unique.index(email) + 1,
                "method": "smtp_pattern",
            }
        if best is None:
            best = {"email": email, "status": result["status"],
                    "reason": result["reason"], "method": "smtp_pattern"}

    # None verified — return first pattern with unknown status
    if best:
        best["patterns_tried"] = len(unique)
        return best
    return {"email": unique[0] if unique else "", "status": STATUS_UNKNOWN,
            "patterns_tried": len(unique), "method": "smtp_pattern"}


def verify_full(email: str, try_smtp: bool = True, try_paid: bool = False,
                 try_neverbounce: bool = False) -> dict:
    """
    Run the full verification pipeline and return a deliverability score
    (0-100) with the breakdown.

    Order: syntax → disposable → DNS/MX → SMTP mailbox → (optional) ZeroBounce.
    Stops early on any definite failure.

    Returns: {
        "email": ...,
        "status": valid|invalid|risky|unknown|disposable,
        "deliverability_score": 0-100,
        "reasons": [list of check-level findings],
        "checks": {"syntax": bool, "not_disposable": bool, "mx": bool, "smtp": ...},
    }
    """
    result = {
        "email": email,
        "status": STATUS_UNKNOWN,
        "deliverability_score": 0,
        "reasons": [],
        "checks": {},
    }

    # 1. Syntax
    if not email or not EMAIL_RE.match(email):
        result["status"] = STATUS_INVALID
        result["reasons"].append("Invalid email syntax")
        result["checks"]["syntax"] = False
        return result
    result["checks"]["syntax"] = True

    # 2. Disposable domain check (hard reject)
    if is_disposable(email):
        result["status"] = STATUS_DISPOSABLE
        result["reasons"].append("Disposable/throwaway domain")
        result["checks"]["not_disposable"] = False
        return result
    result["checks"]["not_disposable"] = True

    # 3. MX check
    mx = verify_mx(email)
    result["checks"]["mx"] = mx.get("has_mx", False)
    if mx["status"] == STATUS_INVALID:
        result["status"] = STATUS_INVALID
        result["reasons"].append(mx.get("reason", "No MX record"))
        return result
    result["reasons"].append(f"MX: {mx.get('reason', 'ok')}")

    # 4. SMTP mailbox check (optional, free but slow)
    if try_smtp:
        smtp = verify_smtp(email, timeout=8)
        result["checks"]["smtp"] = smtp["status"]
        if smtp["status"] == STATUS_INVALID:
            result["status"] = STATUS_INVALID
            result["reasons"].append(smtp.get("reason", "Mailbox not found"))
            return result
        if smtp.get("is_catchall"):
            result["status"] = STATUS_RISKY
            result["reasons"].append("Catch-all domain — mailbox not verifiable")
            result["deliverability_score"] = 55
            return result
        if smtp["status"] == STATUS_VALID:
            result["reasons"].append("SMTP confirmed mailbox exists")

    # 5. NeverBounce (optional, authoritative 3rd gate)
    if try_neverbounce:
        try:
            from src.neverbounce import verify as nb_verify
            nb = nb_verify(email)
            result["neverbounce_result"] = nb.result
            result["neverbounce_safe"] = nb.safe_to_send
            result["checks"]["neverbounce"] = nb.result

            if nb.result == "valid":
                result["status"] = STATUS_VALID
                result["deliverability_score"] = 95
                result["reasons"].append("NeverBounce: VALID")
                return result
            elif nb.result == "invalid":
                result["status"] = STATUS_INVALID
                result["reasons"].append("NeverBounce: INVALID")
                return result
            elif nb.result == "catchall":
                result["status"] = STATUS_RISKY
                result["deliverability_score"] = 55
                result["reasons"].append("NeverBounce: CATCHALL (may bounce)")
            elif nb.result == "disposable":
                result["status"] = STATUS_DISPOSABLE
                result["reasons"].append("NeverBounce: DISPOSABLE")
                return result
        except Exception as e:
            result["reasons"].append(f"NeverBounce error: {e}")

    # 6. Paid ZeroBounce (optional)
    if try_paid:
        zb = verify_zerobounce(email)
        if zb["status"] == STATUS_VALID:
            result["status"] = STATUS_VALID
            result["reasons"].append(f"ZeroBounce: {zb.get('reason', 'valid')}")
            result["deliverability_score"] = 95
            return result
        if zb["status"] == STATUS_INVALID:
            result["status"] = STATUS_INVALID
            result["reasons"].append(f"ZeroBounce: {zb.get('reason', 'invalid')}")
            return result

    # Compute score from what we know
    if result["checks"].get("smtp") == STATUS_VALID:
        result["deliverability_score"] = 85
        result["status"] = STATUS_VALID
    elif mx["status"] == STATUS_VALID:
        result["deliverability_score"] = 65 if try_smtp else 60
        result["status"] = STATUS_VALID  # MX-valid, best we can say without SMTP
    else:
        result["deliverability_score"] = 30
        result["status"] = STATUS_UNKNOWN

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
