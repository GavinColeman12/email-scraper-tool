"""
WHOIS-based cross-verification for scraped emails.

If the domain's WHOIS registrant phone matches the business's listed
phone number, we know the scraped email actually belongs to this
business (not a squatter). This boosts confidence from MEDIUM to HIGH.

Works on ~40% of domains. The rest use privacy protection (WhoisGuard,
REDACTED FOR PRIVACY, etc.) which returns a no-signal result.

Never blocks the pipeline — all failures return no-signal.
"""
import re
import sys
import time

# In-memory cache for this session. Domain → result dict.
_WHOIS_CACHE = {}
_LAST_QUERY_TIME = [0.0]  # list for mutability from closure
_RATE_LIMIT_DELAY = 0.2   # seconds between queries


# Patterns that indicate privacy-protected WHOIS records
_PRIVACY_MARKERS = [
    "redacted", "privacy", "whoisguard", "data protected",
    "domains by proxy", "contact privacy", "private by design",
    "perfect privacy", "gdpr masked", "withheld",
    "not disclosed", "select request link", "see privacypost",
]


def normalize_phone(phone: str) -> str:
    """
    Strip all non-digit characters, return digits only.
    Takes the last 10 digits to compare US numbers consistently
    (tolerates the '1' country code prefix).
    """
    if not phone:
        return ""
    digits = re.sub(r"\D", "", str(phone))
    # Keep last 10 digits (US format). If fewer, return what we have.
    if len(digits) >= 10:
        return digits[-10:]
    return digits


def phones_match(a: str, b: str) -> bool:
    """True if two phones share the same last-10-digit core."""
    na = normalize_phone(a)
    nb = normalize_phone(b)
    if not na or not nb:
        return False
    if len(na) < 10 or len(nb) < 10:
        return False
    return na == nb


def _extract_phone_from_raw_text(raw: str) -> str:
    """
    Parse raw WHOIS text for a registrant/admin/tech phone line.

    Excludes 'Registrar Abuse Contact Phone' — that's the REGISTRAR's
    number (MarkMonitor, GoDaddy, etc.), not the business we're
    verifying. Picking that up would give false positives.
    """
    if not raw:
        return ""

    # Scan line-by-line. Look for labels we trust as the BUSINESS owner's
    # number, and explicitly reject registrar-level labels.
    trusted_labels = [
        r"registrant\s+phone",
        r"registrant\s+contact\s+phone",
        r"admin\s+phone",
        r"administrative\s+phone",
        r"tech\s+phone",
        r"technical\s+phone",
    ]
    rejected_labels = [
        r"registrar\s+abuse\s+contact\s+phone",
        r"registrar\s+phone",
        r"reseller\s+phone",
    ]

    for line in raw.splitlines():
        line_lower = line.lower().strip()
        # Skip rejected labels first
        if any(re.search(rej, line_lower) for rej in rejected_labels):
            continue
        # Does the line start with a trusted phone label?
        if not any(re.search(trust, line_lower) for trust in trusted_labels):
            continue
        # Extract the phone number from this line
        # Format examples: "Registrant Phone: +1.5551234567"
        #                  "Tech Phone: (555) 123-4567"
        phone_match = re.search(
            r"(?:phone[^:]*:)\s*(.+?)(?:\s*\(ext|\s*$)",
            line_lower,
        )
        if phone_match:
            candidate = phone_match.group(1).strip()
            # Skip if privacy-protected
            if _is_privacy_protected(candidate):
                continue
            # Must have at least 7 digits to be plausible
            digits = re.sub(r"\D", "", candidate)
            if len(digits) >= 7:
                return candidate
    return ""


def _is_privacy_protected(text: str) -> bool:
    """Return True if any privacy marker appears in the WHOIS text field."""
    if not text:
        return False
    low = str(text).lower()
    return any(marker in low for marker in _PRIVACY_MARKERS)


def lookup_whois_phone(domain: str) -> dict:
    """
    Run a WHOIS lookup on the domain and extract the registrant phone.

    Returns:
      {
        "phone": str or "",
        "registrant_name": str or "",
        "privacy_protected": bool,
        "error": str or "",
      }

    Cached per-session.
    """
    if not domain:
        return {"phone": "", "registrant_name": "",
                "privacy_protected": False, "error": "no domain"}

    domain = domain.lower().strip()
    if domain in _WHOIS_CACHE:
        return _WHOIS_CACHE[domain]

    # Rate-limit politely
    elapsed = time.time() - _LAST_QUERY_TIME[0]
    if elapsed < _RATE_LIMIT_DELAY:
        time.sleep(_RATE_LIMIT_DELAY - elapsed)

    result = {"phone": "", "registrant_name": "",
              "privacy_protected": False, "error": ""}

    try:
        import whois
    except ImportError:
        result["error"] = "python-whois not installed"
        _WHOIS_CACHE[domain] = result
        return result

    try:
        _LAST_QUERY_TIME[0] = time.time()
        w = whois.whois(domain)
    except Exception as e:
        msg = str(e)[:100]
        result["error"] = f"whois failed: {msg}"
        print(f"[whois_verifier] {domain}: {msg}", file=sys.stderr)
        _WHOIS_CACHE[domain] = result
        return result

    # Extract phone — field name varies across registrars
    phone = ""
    for field in ("phone", "registrant_phone", "tech_phone", "admin_phone"):
        val = getattr(w, field, None) if hasattr(w, field) else w.get(field) if hasattr(w, "get") else None
        if val:
            # Sometimes it's a list of phones
            if isinstance(val, (list, tuple)):
                val = val[0] if val else ""
            phone_candidate = str(val).strip()
            if phone_candidate and not _is_privacy_protected(phone_candidate):
                phone = phone_candidate
                break

    # Fallback: parse raw WHOIS text for registrant/admin/tech phone lines.
    # IMPORTANT: we explicitly skip "Registrar Abuse Contact Phone" because
    # that's the registrar's number (e.g. MarkMonitor), NOT the business.
    if not phone:
        phone = _extract_phone_from_raw_text(str(w))

    # Extract registrant name
    name = ""
    for field in ("name", "registrant_name", "org"):
        val = getattr(w, field, None) if hasattr(w, field) else w.get(field) if hasattr(w, "get") else None
        if val:
            if isinstance(val, (list, tuple)):
                val = val[0] if val else ""
            name_candidate = str(val).strip()
            if name_candidate and not _is_privacy_protected(name_candidate):
                name = name_candidate
                break

    # Detect privacy protection
    all_fields_text = ""
    for field in ("phone", "registrant_phone", "name", "registrant_name", "org"):
        val = getattr(w, field, None) if hasattr(w, field) else w.get(field) if hasattr(w, "get") else None
        if val:
            if isinstance(val, (list, tuple)):
                val = " ".join(str(v) for v in val)
            all_fields_text += " " + str(val)

    if _is_privacy_protected(all_fields_text):
        result["privacy_protected"] = True

    result["phone"] = phone
    result["registrant_name"] = name

    # If we got nothing usable AND it's not obviously privacy-protected,
    # it might just be a registrar that hides by default
    if not phone and not name and not result["privacy_protected"]:
        result["privacy_protected"] = True  # treat as privacy for safety

    _WHOIS_CACHE[domain] = result
    return result


def verify_against_business_phone(domain: str, business_phone: str) -> dict:
    """
    Verify a domain's WHOIS phone matches the business's listed phone.

    Returns:
      {
        "matches": True | False | None,    # None = no signal (privacy/error)
        "registrant_phone": str,
        "privacy_protected": bool,
        "reason": str,                     # human-readable status
      }
    """
    if not domain or not business_phone:
        return {"matches": None, "registrant_phone": "",
                "privacy_protected": False,
                "reason": "missing domain or business phone"}

    whois_result = lookup_whois_phone(domain)

    if whois_result.get("error"):
        return {"matches": None, "registrant_phone": "",
                "privacy_protected": False,
                "reason": f"whois error: {whois_result['error']}"}

    if whois_result.get("privacy_protected"):
        return {"matches": None, "registrant_phone": "",
                "privacy_protected": True,
                "reason": "WHOIS is privacy-protected — no signal"}

    registrant_phone = whois_result.get("phone", "")
    if not registrant_phone:
        return {"matches": None, "registrant_phone": "",
                "privacy_protected": False,
                "reason": "no registrant phone in WHOIS"}

    if phones_match(registrant_phone, business_phone):
        return {"matches": True, "registrant_phone": registrant_phone,
                "privacy_protected": False,
                "reason": "WHOIS registrant phone matches business phone"}
    else:
        return {"matches": False, "registrant_phone": registrant_phone,
                "privacy_protected": False,
                "reason": f"WHOIS phone {registrant_phone} does not match "
                          f"business phone {business_phone}"}
