"""
email_scoring.py — Composable scoring system for email candidates.

Replaces the monolithic _candidate_confidence() / compute_lead_quality_score()
with a structured, multi-dimensional score that captures what we actually know
about each email candidate. Exposes granular fields downstream gates can
compose instead of grading by letter.

Design principles:
  1. Specificity is a cap, not an additive term. `info@` can't out-score
     `drjones@` by being scraped harder.
  2. Validity signals degrade on catchall domains. NB=valid on catchall
     means "server accepts everything," not "mailbox exists."
  3. Independent signal agreement is multiplicative — synergy bonuses
     reward when scraped + triangulated + NB=valid all agree.
  4. Structured output (score + specificity + is_triangulated + is_catchall
     + requires_manual_review). Downstream gates compose these fields;
     letter grades are UI display only.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1: SPECIFICITY CLASSIFIER
# ═════════════════════════════════════════════════════════════════════════════

class Specificity(str, Enum):
    PERSONAL = "personal"              # jsmith@, tim.jones@, dr.smith@
    TARGETED_ROLE = "targeted_role"    # tim.manager@, jess.office@
    GENERIC_ROLE = "generic_role"      # marketing@, billing@, support@
    GENERIC_INBOX = "generic_inbox"    # info@, hello@, contact@
    UNKNOWN = "unknown"


GENERIC_INBOX_LOCALS = {
    "info", "hello", "contact", "admin", "office", "mail", "help",
    "inquiries", "general", "main", "front", "frontdesk", "reception",
}

GENERIC_ROLE_LOCALS = {
    "support", "sales", "billing", "accounts", "accounting", "hr",
    "careers", "jobs", "marketing", "newsletter", "appointments",
    "booking", "scheduling", "service", "services", "customer",
    "customerservice", "feedback", "reviews", "team", "staff",
}

ROLE_TOKENS = {
    "manager", "director", "admin", "assistant", "coordinator",
    "office", "billing", "reception", "marketing",
}

_COMMON_NON_NAMES = {
    "smile", "smiles", "hello", "email", "today", "happy",
    "thanks", "bright", "perfect", "modern", "gentle", "family",
    "dental", "clinic", "office", "center", "premier",
}


def classify_specificity(
    email: str,
    owner_first: str = "",
    owner_last: str = "",
) -> Specificity:
    """Deterministic classifier. Returns one of the five Specificity values."""
    if not email or "@" not in email:
        return Specificity.UNKNOWN

    local = email.split("@")[0].lower().strip()
    if not local:
        return Specificity.UNKNOWN

    if local in GENERIC_INBOX_LOCALS:
        return Specificity.GENERIC_INBOX

    if local in GENERIC_ROLE_LOCALS:
        return Specificity.GENERIC_ROLE
    if "." in local:
        parts = local.split(".")
        if all(p in GENERIC_ROLE_LOCALS or p in GENERIC_INBOX_LOCALS for p in parts):
            return Specificity.GENERIC_ROLE

    if owner_first and owner_last:
        f = owner_first.lower()
        l = owner_last.lower()
        personal_patterns = {
            f"{f}.{l}", f"{f}{l}",
            f"{f[0]}{l}" if f else None,
            f"{f[0]}.{l}" if f else None,
            l, f,
            f"{l}.{f}",
            f"{l}{f[0]}" if f else None,
            f"dr.{l}", f"dr{l}",
        }
        personal_patterns.discard(None)
        if local in personal_patterns:
            return Specificity.PERSONAL

    if "." in local:
        parts = local.split(".")
        if len(parts) == 2:
            left, right = parts
            right_is_role = right in ROLE_TOKENS or right in GENERIC_ROLE_LOCALS
            if right_is_role and _looks_like_name_token(left):
                return Specificity.TARGETED_ROLE
            if left in ROLE_TOKENS and _looks_like_name_token(right):
                return Specificity.TARGETED_ROLE

    if _looks_personal(local):
        return Specificity.PERSONAL

    if re.match(r"^(dr|mr|mrs|ms|miss|prof)\.?([a-z]+)$", local):
        return Specificity.PERSONAL

    if local.isalpha() and len(local) <= 12:
        return Specificity.GENERIC_ROLE

    return Specificity.UNKNOWN


def _looks_like_name_token(s: str) -> bool:
    if not s or len(s) < 2:
        return False
    if s in GENERIC_INBOX_LOCALS or s in GENERIC_ROLE_LOCALS:
        return False
    if s in ROLE_TOKENS:
        return False
    return s.isalpha() and 2 <= len(s) <= 15


def _looks_personal(local: str) -> bool:
    if re.match(r"^[a-z]{2,15}\.[a-z]{2,15}$", local):
        a, b = local.split(".")
        if a not in GENERIC_ROLE_LOCALS and b not in GENERIC_ROLE_LOCALS:
            return True
    if re.match(r"^[a-z][a-z]{4,14}$", local):
        if local in GENERIC_ROLE_LOCALS or local in GENERIC_INBOX_LOCALS:
            return False
        if local in _COMMON_NON_NAMES:
            return False
        return True
    return False


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2: INPUTS / OUTPUT
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class ScoringInputs:
    email: str
    owner_first: str = ""
    owner_last: str = ""
    owner_confidence: int = 0
    owner_title: str = ""
    was_scraped_direct: bool = False
    was_found_via_search: bool = False
    was_generated_from_pattern: bool = True
    pattern_triangulated: bool = False
    pattern_confidence: int = 0
    pattern_evidence_count: int = 0
    nb_valid: bool = False
    nb_invalid: bool = False
    nb_catchall: bool = False
    nb_unknown: bool = False
    smtp_valid: bool = False
    smtp_catchall: bool = False
    is_catchall_domain: bool = False
    is_workspace_or_o365: bool = False
    owner_last_name_in_business: bool = False


@dataclass
class EmailScore:
    score: int
    score_range_low: int
    score_range_high: int
    specificity: Specificity
    is_catchall: bool
    is_triangulated: bool                   # structured field — no string grep
    requires_manual_review: bool
    grade: str
    components: dict[str, int] = field(default_factory=dict)
    evidence: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["specificity"] = self.specificity.value
        return d


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3: SCORER
# ═════════════════════════════════════════════════════════════════════════════

SPECIFICITY_CAPS = {
    Specificity.PERSONAL: 30,
    Specificity.TARGETED_ROLE: 25,
    Specificity.GENERIC_ROLE: 20,
    Specificity.GENERIC_INBOX: 15,
    Specificity.UNKNOWN: 15,
}


def score_email_candidate(inputs: ScoringInputs) -> EmailScore:
    components: dict[str, int] = {}
    evidence: list[str] = []
    warnings: list[str] = []

    specificity = classify_specificity(
        inputs.email, inputs.owner_first, inputs.owner_last
    )
    cap = SPECIFICITY_CAPS[specificity]

    # Source evidence (capped by specificity)
    source_pts = 0
    if inputs.was_scraped_direct:
        source_pts = 30
        evidence.append("📎 Scraped directly from website")
    elif inputs.was_found_via_search:
        source_pts = 22
        evidence.append("🔎 Found via Google/LinkedIn search")
    elif inputs.pattern_triangulated:
        source_pts = 18
        evidence.append("🧮 Pattern-generated, triangulation-backed")
    elif inputs.was_generated_from_pattern:
        source_pts = 12
        evidence.append("🧮 Constructed from pattern rules (no triangulation)")

    source_pts_capped = min(source_pts, cap)
    if source_pts > cap:
        warnings.append(
            f"Source value capped at {cap} due to {specificity.value} mailbox"
        )
    components["source_evidence"] = source_pts_capped

    # Triangulation
    tri_pts = 0
    if inputs.pattern_triangulated:
        if inputs.pattern_evidence_count >= 3:
            tri_pts = 22
        elif inputs.pattern_evidence_count == 2:
            tri_pts = 18
        else:
            tri_pts = 12
        evidence.append(
            f"✅ Pattern triangulated from {inputs.pattern_evidence_count} "
            f"independent email(s) at domain"
        )
    components["triangulation"] = tri_pts

    # Verification (NB is authoritative, SMTP is corroborating)
    if inputs.nb_invalid:
        components["verification"] = -100
        evidence.append("❌ NeverBounce: INVALID (hard fail)")
        return _build_score(
            score=0, specificity=specificity,
            is_catchall=inputs.is_catchall_domain,
            is_triangulated=inputs.pattern_triangulated,
            components=components, evidence=evidence, warnings=warnings,
            requires_manual_review=False,
        )

    verify_pts = 0
    if inputs.nb_valid:
        if inputs.is_catchall_domain:
            verify_pts = 5
            evidence.append("⚠️ NeverBounce: VALID but catchall domain (weak signal)")
        else:
            verify_pts = 25
            evidence.append("✅ NeverBounce: VALID")
    elif inputs.nb_catchall:
        verify_pts = 8
        evidence.append("⚠️ NeverBounce: CATCHALL (cannot verify mailbox)")
    elif inputs.nb_unknown:
        verify_pts = 2
        evidence.append("❓ NeverBounce: UNKNOWN")

    if inputs.smtp_valid and not inputs.smtp_catchall:
        verify_pts += 5
        evidence.append("✅ SMTP: accepted (non-catchall)")
    elif inputs.smtp_catchall:
        warnings.append("SMTP indicates catchall")

    components["verification"] = verify_pts

    # Owner context
    owner_pts = 0
    if inputs.owner_first and inputs.owner_last:
        owner_pts += 5
        if inputs.owner_confidence >= 80:
            owner_pts += 5
        elif inputs.owner_confidence >= 60:
            owner_pts += 3
        if inputs.owner_title:
            owner_pts += 5
            evidence.append(
                f"👤 Owner identified: {inputs.owner_first} "
                f"{inputs.owner_last} ({inputs.owner_title})"
            )
        else:
            evidence.append(
                f"👤 Owner identified: {inputs.owner_first} {inputs.owner_last}"
            )
        if inputs.owner_last_name_in_business:
            owner_pts += 8
            evidence.append("⭐ Owner surname matches business name")
    components["owner_context"] = owner_pts

    # Synergy bonus — independent signal agreement
    synergy_pts = 0
    if (
        inputs.was_scraped_direct
        and inputs.pattern_triangulated
        and inputs.nb_valid
        and not inputs.is_catchall_domain
    ):
        synergy_pts = 15
        evidence.append("🎯🎯 Triple-source agreement: scraped + pattern + NB")
    elif (
        inputs.pattern_triangulated
        and inputs.pattern_confidence >= 70
        and inputs.nb_valid
        and not inputs.is_catchall_domain
    ):
        synergy_pts = 10
        evidence.append(
            "🎯 Synergy bonus: triangulation + NB=valid + non-catchall"
        )
    elif (
        inputs.was_scraped_direct
        and inputs.nb_valid
        and not inputs.is_catchall_domain
    ):
        synergy_pts = 7
        evidence.append("🎯 Agreement bonus: scraped-direct + NB=valid")
    components["synergy"] = synergy_pts

    # Catchall guess penalty
    if (
        inputs.was_generated_from_pattern
        and not inputs.was_scraped_direct
        and not inputs.was_found_via_search
        and inputs.is_catchall_domain
        and not inputs.pattern_triangulated
    ):
        components["catchall_guess_penalty"] = -10
        warnings.append(
            "Generated guess on catchall domain without triangulation — "
            "high false-positive risk"
        )

    raw_score = sum(v for v in components.values() if v != -100)
    score = max(0, min(100, raw_score))

    # Uncertainty bands
    num_strong = sum([
        inputs.nb_valid and not inputs.is_catchall_domain,
        inputs.was_scraped_direct,
        inputs.pattern_triangulated and inputs.pattern_evidence_count >= 2,
    ])
    band = 5 if num_strong >= 2 else (10 if num_strong == 1 else 15)
    score_low = max(0, score - band)
    score_high = min(100, score + band)

    # Manual review flags
    requires_review = False
    review_reasons = []
    if inputs.is_catchall_domain and not inputs.pattern_triangulated:
        requires_review = True
        review_reasons.append("catchall domain without triangulation")
    if 60 <= score <= 74 and specificity in (
        Specificity.PERSONAL, Specificity.TARGETED_ROLE
    ):
        requires_review = True
        review_reasons.append("borderline score on personal-looking email")
    if specificity == Specificity.UNKNOWN:
        requires_review = True
        review_reasons.append("unclassifiable mailbox specificity")
    if inputs.owner_last_name_in_business and inputs.owner_confidence < 40:
        requires_review = True
        review_reasons.append("business name match but weak owner identification")

    if review_reasons:
        warnings.append("Manual review: " + "; ".join(review_reasons))

    return _build_score(
        score=score,
        score_range_low=score_low,
        score_range_high=score_high,
        specificity=specificity,
        is_catchall=inputs.is_catchall_domain,
        is_triangulated=inputs.pattern_triangulated,
        components=components,
        evidence=evidence,
        warnings=warnings,
        requires_manual_review=requires_review,
    )


def _build_score(
    score: int,
    specificity: Specificity,
    is_catchall: bool,
    is_triangulated: bool,
    components: dict,
    evidence: list,
    warnings: list,
    requires_manual_review: bool,
    score_range_low: Optional[int] = None,
    score_range_high: Optional[int] = None,
) -> EmailScore:
    low = score_range_low if score_range_low is not None else max(0, score - 10)
    high = score_range_high if score_range_high is not None else min(100, score + 10)
    return EmailScore(
        score=score,
        score_range_low=low,
        score_range_high=high,
        specificity=specificity,
        is_catchall=is_catchall,
        is_triangulated=is_triangulated,
        requires_manual_review=requires_manual_review,
        grade=_to_grade(score),
        components=components,
        evidence=evidence,
        warnings=warnings,
    )


def _to_grade(score: int) -> str:
    if score >= 75: return "A"
    if score >= 60: return "B"
    if score >= 45: return "C"
    if score >= 20: return "D"
    return "F"


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4: DECISION GATE
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class SendDecision:
    should_send: bool
    should_verify_further: bool
    should_manual_review: bool
    should_skip: bool
    reason: str


def gate_decision(
    score: EmailScore,
    min_score: int = 70,
    allow_catchall_with_triangulation: bool = True,
    block_generic_inboxes: bool = True,
) -> SendDecision:
    if score.score == 0:
        return SendDecision(False, False, False, True,
                            reason="NeverBounce returned INVALID")

    if block_generic_inboxes and score.specificity == Specificity.GENERIC_INBOX:
        return SendDecision(False, False, False, True,
                            reason="Generic inbox (info@, contact@) — low authority")

    if score.requires_manual_review and score.score < min_score + 10:
        return SendDecision(False, False, True, False,
                            reason="Flagged for manual review: "
                                   + "; ".join(score.warnings))

    if score.is_catchall:
        if score.is_triangulated and allow_catchall_with_triangulation:
            if score.score >= min_score:
                return SendDecision(True, False, False, False,
                                    reason="Catchall but triangulation-confirmed")
            return SendDecision(False, True, False, False,
                                reason="Catchall + borderline — verify further")
        return SendDecision(False, True, False, False,
                            reason="Catchall without triangulation — verify further")

    if score.score >= min_score:
        return SendDecision(True, False, False, False,
                            reason=f"Score {score.score} ≥ threshold {min_score}")

    if score.score >= min_score - 15:
        return SendDecision(False, False, True, False,
                            reason=f"Score {score.score} borderline — review")

    return SendDecision(False, False, False, True,
                        reason=f"Score {score.score} below threshold")


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5: FRESHNESS DECAY
# ═════════════════════════════════════════════════════════════════════════════

def decay_score_by_age(score: int, age_days: int) -> int:
    """-2 points per 30 days. Business emails change when people switch jobs."""
    months = age_days / 30.0
    decayed = score - (2 * months)
    return max(0, int(decayed))


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 6: ADAPTER — pipeline candidate dict → ScoringInputs
# ═════════════════════════════════════════════════════════════════════════════

def scoring_inputs_from_pipeline_candidate(
    candidate: dict,
    owner_first: str,
    owner_last: str,
    owner_confidence: int,
    owner_title: str,
    pattern_confidence: int,
    pattern_evidence_count: int,
    is_catchall_domain: bool,
    owner_last_name_in_business: bool,
) -> ScoringInputs:
    source = (candidate.get("source") or "").lower()
    nb_result = (candidate.get("nb_result") or "").lower()

    scraped_direct_sources = {"scraped_direct", "scraped", "website_scrape"}
    search_sources_keywords = ("google", "linkedin", "search")

    return ScoringInputs(
        email=candidate.get("email", ""),
        owner_first=owner_first,
        owner_last=owner_last,
        owner_confidence=owner_confidence,
        owner_title=owner_title,
        was_scraped_direct=(source in scraped_direct_sources),
        was_found_via_search=any(k in source for k in search_sources_keywords),
        was_generated_from_pattern=(
            source in ("first_last_fallback", "industry_prior", "detected_pattern")
        ),
        pattern_triangulated=(source == "detected_pattern"),
        pattern_confidence=pattern_confidence,
        pattern_evidence_count=pattern_evidence_count,
        nb_valid=bool(candidate.get("nb_valid")) or nb_result == "valid",
        nb_invalid=nb_result == "invalid",
        nb_catchall=nb_result == "catchall",
        nb_unknown=nb_result in ("unknown", ""),
        smtp_valid=bool(candidate.get("smtp_valid")),
        smtp_catchall=bool(candidate.get("smtp_catchall")),
        is_catchall_domain=is_catchall_domain,
        owner_last_name_in_business=owner_last_name_in_business,
    )
