"""
NeverBounce integration — the third gate in our verification waterfall.
Use after MX + SMTP to get authoritative deliverability signal.

Pricing: Free 1,000/month, then $0.003 per verification.
Free tier enough for first ~1,000 emails — plenty to validate the pipeline.
"""
import os
import time
from dataclasses import dataclass, field
from typing import List, Optional

import requests

NEVERBOUNCE_URL = "https://api.neverbounce.com/v4/single/check"
NEVERBOUNCE_ACCOUNT_URL = "https://api.neverbounce.com/v4/account/info"


@dataclass
class NeverBounceResult:
    email: str
    result: str  # valid | invalid | disposable | catchall | unknown
    flags: List[str] = field(default_factory=list)
    suggested_correction: Optional[str] = None
    safe_to_send: bool = False
    retry_token: Optional[str] = None


def _get_api_key(api_key=None):
    """Resolve API key from arg, src.secrets, or env var."""
    if api_key:
        return api_key
    try:
        from src.secrets import get_secret
        return get_secret("NEVERBOUNCE_API_KEY")
    except Exception:
        return os.getenv("NEVERBOUNCE_API_KEY")


def verify(email, api_key=None, timeout=10):
    """
    Verify a single email via NeverBounce API.

    Returns NeverBounceResult. If no API key configured, returns 'unknown'.
    """
    key = _get_api_key(api_key)
    if not key:
        return NeverBounceResult(email=email, result="unknown", flags=["no_api_key"])

    try:
        response = requests.get(
            NEVERBOUNCE_URL,
            params={"key": key, "email": email, "timeout": 10},
            timeout=timeout,
        )

        if response.status_code == 429:
            return NeverBounceResult(email=email, result="unknown", flags=["rate_limited"])

        data = response.json()

        if data.get("status") != "success":
            return NeverBounceResult(
                email=email,
                result="unknown",
                flags=["api_error", data.get("message", "")],
            )

        result = data.get("result", "unknown")
        flags = data.get("flags", []) or []

        # Only "valid" is truly safe. catchall/unknown are risky.
        safe_to_send = result == "valid"

        return NeverBounceResult(
            email=email,
            result=result,
            flags=flags,
            suggested_correction=data.get("suggested_correction") or None,
            safe_to_send=safe_to_send,
        )

    except requests.exceptions.Timeout:
        return NeverBounceResult(email=email, result="unknown", flags=["timeout"])
    except Exception as e:
        return NeverBounceResult(email=email, result="unknown", flags=["error", str(e)])


def verify_batch(emails, api_key=None, delay=0.1):
    """Verify multiple emails. Small delay between requests to be polite."""
    results = {}
    for email in emails:
        results[email] = verify(email, api_key=api_key)
        time.sleep(delay)
    return results


def get_account_info(api_key=None):
    """Check remaining credits. Useful to display in UI."""
    key = _get_api_key(api_key)
    if not key:
        return {"error": "No API key"}

    try:
        response = requests.get(
            NEVERBOUNCE_ACCOUNT_URL,
            params={"key": key},
            timeout=5,
        )
        return response.json()
    except Exception as e:
        return {"error": str(e)}


def is_available():
    """True if a NeverBounce API key is configured."""
    return bool(_get_api_key())
