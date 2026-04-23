"""
DNS MX record pre-check.

Free via standard DNS (~10 ms uncached, ~0 ms cached). When a domain
has no MX records, the domain cannot receive mail — skipping the
NeverBounce call on such a domain saves $0.003 per dead domain.

Cheap in-memory cache: 1-hour TTL per domain.
"""
from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

# domain → (has_mx: bool, checked_at: float)
_MX_CACHE: dict[str, tuple[bool, float]] = {}
_MX_TTL_SEC = 3600  # 1 hour — MX changes are rare


def domain_has_mx(domain: str) -> bool:
    """
    Return True if the domain has at least one MX record (i.e. can
    receive mail). Returns True on lookup failure — we fail open so
    a transient DNS hiccup doesn't skip valid domains.
    """
    if not domain:
        return True  # fail-open
    dom = domain.lower().strip().lstrip(".")
    now = time.time()
    hit = _MX_CACHE.get(dom)
    if hit and (now - hit[1]) < _MX_TTL_SEC:
        return hit[0]
    try:
        import dns.resolver  # type: ignore
        answers = dns.resolver.resolve(dom, "MX", lifetime=3.0)
        ok = len(answers) > 0
    except Exception as e:
        # NoAnswer / NXDOMAIN / DNSException — domain has no MX records
        name = type(e).__name__
        if name in ("NoAnswer", "NXDOMAIN", "NoNameservers"):
            ok = False
        else:
            # Timeout or other transient — fail-open so we don't wrongly
            # skip a real domain because of a flaky resolver.
            logger.debug(f"MX lookup transient error for {dom}: {e}")
            ok = True
    _MX_CACHE[dom] = (ok, now)
    return ok


def email_has_mx(email: str) -> bool:
    if not email or "@" not in email:
        return False
    return domain_has_mx(email.split("@", 1)[1])
