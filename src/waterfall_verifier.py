"""
Waterfall verifier — conservative multi-gate system for determining if an email
is safe to send.

Gates:
1. Syntax + domain (MX records exist)
2. SMTP RCPT TO (with catch-all detection)
3. NeverBounce (authoritative third party, optional)
4. Cross-source corroboration (website, NPI, pattern detection, Hunter)

Final verdict:
- SAFE_TO_SEND: score >= 70 (multiple signals passed, including at least one authoritative)
- RISKY: score 40-69 (some signals pass but gaps exist)
- UNVERIFIABLE: score 20-39 (not enough signal either way)
- DO_NOT_SEND: score < 20 or hard invalid signal
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from src.email_verifier import verify_mx, verify_smtp, is_known_catchall_mx
from src.neverbounce import verify as neverbounce_verify


class SendVerdict(str, Enum):
    SAFE_TO_SEND = "safe_to_send"
    RISKY = "risky"
    DO_NOT_SEND = "do_not_send"
    UNVERIFIABLE = "unverifiable"


@dataclass
class WaterfallResult:
    email: str
    verdict: SendVerdict
    confidence: int  # 0-100

    # Gate results
    mx_valid: bool = False
    smtp_valid: bool = False
    smtp_catchall: bool = False
    neverbounce_result: Optional[str] = None
    neverbounce_safe: bool = False

    # Cross-source corroboration
    found_on_website: bool = False
    pattern_detected_from_domain: bool = False
    name_matches_npi_provider: bool = False
    appears_in_hunter: bool = False

    # Reasoning
    reasons_pass: List[str] = field(default_factory=list)
    reasons_fail: List[str] = field(default_factory=list)

    # Cost tracking
    neverbounce_used: bool = False


def verify_waterfall(email, corroboration=None, use_neverbounce=True, skip_smtp=False):
    """
    Run the full waterfall.

    corroboration: dict with keys:
        - found_on_website: bool
        - pattern_detected_from_domain: bool
        - name_matches_npi_provider: bool
        - appears_in_hunter: bool
    """
    corroboration = corroboration or {}
    result = WaterfallResult(email=email, verdict=SendVerdict.UNVERIFIABLE, confidence=0)

    # ── Gate 1: MX records ────────────────────────────────────────────
    try:
        mx_result = verify_mx(email)
        result.mx_valid = mx_result.get("status") == "valid"
        if result.mx_valid:
            result.reasons_pass.append("MX records exist")
        else:
            result.reasons_fail.append(f"MX check failed: {mx_result.get('reason', '')}")
            result.verdict = SendVerdict.DO_NOT_SEND
            result.confidence = 0
            return result
    except Exception as e:
        result.reasons_fail.append(f"MX error: {e}")
        result.verdict = SendVerdict.DO_NOT_SEND
        return result

    # ── Gate 2: SMTP RCPT TO ──────────────────────────────────────────
    if not skip_smtp:
        try:
            smtp_result = verify_smtp(email, timeout=10)
            result.smtp_valid = smtp_result.get("status") == "valid"
            result.smtp_catchall = bool(smtp_result.get("is_catchall", False))

            if result.smtp_valid and not result.smtp_catchall:
                result.reasons_pass.append("SMTP RCPT TO accepted (not catch-all)")
            elif result.smtp_catchall:
                result.reasons_fail.append(
                    "SMTP server is catch-all — cannot verify address exists"
                )
            elif smtp_result.get("status") == "invalid":
                result.reasons_fail.append(
                    f"SMTP rejected: {smtp_result.get('reason', '')}"
                )
            else:
                result.reasons_fail.append(
                    f"SMTP unknown: {smtp_result.get('reason', '')}"
                )
        except Exception as e:
            result.reasons_fail.append(f"SMTP error (may be port 25 blocked): {e}")

    # ── Gate 3: NeverBounce (authoritative, optional) ────────────────
    if use_neverbounce:
        nb = neverbounce_verify(email)
        result.neverbounce_used = True
        result.neverbounce_result = nb.result
        result.neverbounce_safe = nb.safe_to_send

        if nb.result == "valid":
            result.reasons_pass.append("NeverBounce: VALID")
        elif nb.result == "invalid":
            result.reasons_fail.append("NeverBounce: INVALID — will bounce")
            result.verdict = SendVerdict.DO_NOT_SEND
            result.confidence = 0
            return result
        elif nb.result == "catchall":
            result.reasons_fail.append("NeverBounce: CATCHALL — may bounce")
        elif nb.result == "disposable":
            result.reasons_fail.append("NeverBounce: DISPOSABLE — not a real inbox")
            result.verdict = SendVerdict.DO_NOT_SEND
            result.confidence = 0
            return result

    # ── Gate 4: Cross-source corroboration ────────────────────────────
    result.found_on_website = bool(corroboration.get("found_on_website", False))
    result.pattern_detected_from_domain = bool(
        corroboration.get("pattern_detected_from_domain", False)
    )
    result.name_matches_npi_provider = bool(
        corroboration.get("name_matches_npi_provider", False)
    )
    result.appears_in_hunter = bool(corroboration.get("appears_in_hunter", False))

    if result.found_on_website:
        result.reasons_pass.append("Email scraped directly from business website")
    if result.pattern_detected_from_domain:
        result.reasons_pass.append("Pattern detected from existing domain emails")
    if result.name_matches_npi_provider:
        result.reasons_pass.append("Name matches verified NPI provider")
    if result.appears_in_hunter:
        result.reasons_pass.append("Email appears in Hunter.io database")

    # ── Scoring ───────────────────────────────────────────────────────
    score = 0

    # Authoritative signals
    if result.neverbounce_safe:
        score += 50
    elif result.neverbounce_result == "catchall":
        score -= 15

    # Direct evidence
    if result.found_on_website:
        score += 40
    if result.appears_in_hunter:
        score += 30
    if result.pattern_detected_from_domain:
        score += 25
    if result.name_matches_npi_provider:
        score += 20

    # Structural checks
    if result.smtp_valid and not result.smtp_catchall:
        score += 15
    elif result.smtp_catchall:
        score -= 5
    if result.mx_valid:
        score += 5

    score = max(0, min(score, 100))
    result.confidence = score

    # Verdict
    if score >= 70:
        result.verdict = SendVerdict.SAFE_TO_SEND
    elif score >= 40:
        result.verdict = SendVerdict.RISKY
    elif score >= 20:
        result.verdict = SendVerdict.UNVERIFIABLE
    else:
        result.verdict = SendVerdict.DO_NOT_SEND

    return result
