"""
Lead quality scoring — thin compatibility wrapper around src.email_scoring.

Historical layout (rating/reviews/website as top-level components) is gone.
The identity-heavy scorer lives in `email_scoring.py`; this module maps the
business-dict shape used by storage and UI pages into ScoringInputs and
delegates.

Public surface kept for backward compatibility:
  - compute_lead_quality_score(business) -> {score, breakdown, tier}
  - rank_businesses(list, top_n) -> list
"""
from __future__ import annotations

from typing import Optional

from .email_scoring import (
    ScoringInputs,
    score_email_candidate,
    Specificity,
)


def _split_owner_name(full_name: str) -> tuple[str, str]:
    parts = (full_name or "").strip().split()
    if len(parts) >= 2:
        return parts[0], parts[-1]
    if len(parts) == 1:
        return parts[0], ""
    return "", ""


def _map_confidence_to_int(conf_str: str) -> int:
    return {"high": 90, "medium": 70, "low": 40}.get(
        (conf_str or "").lower(), 0
    )


def _derive_source_flags(email_source: str) -> tuple[bool, bool, bool, bool]:
    """Return (was_scraped_direct, was_found_via_search,
    was_generated_from_pattern, pattern_triangulated)."""
    s = (email_source or "").lower()
    scraped = any(k in s for k in (
        "scraped", "mailto", "regex", "personal_email",
        "team_page_decision_maker", "team_page_person",
        "team_page_verified",
    ))
    search = any(k in s for k in ("linkedin", "google_search"))
    # "constructed_from_pattern" or "constructed_from_linkedin" or
    # "constructed_from_website_*" all count as pattern-generated.
    pattern_generated = "constructed" in s or s in (
        "detected_pattern", "first_last_fallback", "industry_prior", "",
    )
    triangulated = s == "detected_pattern" or "triangulated" in s
    return scraped, search, pattern_generated, triangulated


def _nb_flags(email_status: str) -> tuple[bool, bool, bool, bool]:
    """(nb_valid, nb_invalid, nb_catchall, nb_unknown)."""
    st = (email_status or "").lower()
    return (
        st == "valid",
        st in ("invalid", "disposable"),
        st in ("risky", "catchall"),
        st in ("", "unknown"),
    )


def _business_dict_to_inputs(business: dict) -> Optional[ScoringInputs]:
    email = (
        business.get("best_email")
        or business.get("primary_email")
        or business.get("email")
        or ""
    )
    if not email:
        return None

    owner_full = business.get("contact_name") or ""
    owner_first, owner_last = _split_owner_name(owner_full)
    owner_title = (business.get("contact_title") or "").strip()
    owner_conf = _map_confidence_to_int(
        business.get("contact_confidence") or business.get("owner_confidence") or ""
    )

    business_name = (business.get("business_name") or "").lower()
    last_in_business = bool(
        owner_last and owner_last.lower() in business_name
    )

    scraped, search, generated, triangulated = _derive_source_flags(
        business.get("email_source") or ""
    )
    nb_valid, nb_invalid, nb_catchall, nb_unknown = _nb_flags(
        business.get("email_status") or ""
    )

    # Triangulation applies PER-EMAIL. A scraped info@ at a domain that
    # has a triangulated pattern for SOMEONE ELSE'S email doesn't inherit
    # that proof. So: only credit triangulation if this specific email's
    # local-part is consistent with the detected pattern for this owner.
    prof = business.get("professional_ids") or {}
    detected = (prof or {}).get("detected_pattern") or {}
    local = email.split("@", 1)[0].lower() if "@" in email else ""
    email_matches_pattern = False
    if detected and owner_first and owner_last and local:
        f, l = owner_first.lower(), owner_last.lower()
        pat_to_local = {
            "first.last": f"{f}.{l}",
            "firstlast": f"{f}{l}",
            "flast": f"{f[0]}{l}" if f else "",
            "f.last": f"{f[0]}.{l}" if f else "",
            "first": f,
            "last": l,
            "drlast": f"dr{l}",
            "dr.last": f"dr.{l}",
            "last.first": f"{l}.{f}",
            "lastf": f"{l}{f[0]}" if f else "",
        }
        expected = pat_to_local.get(detected.get("pattern", "").lower(), "")
        email_matches_pattern = bool(expected) and local == expected

    pattern_confidence = int(detected.get("confidence") or 0) if email_matches_pattern else 0
    pattern_evidence_count = (
        len(detected.get("evidence_emails") or [])
        if email_matches_pattern else 0
    )

    return ScoringInputs(
        email=email,
        owner_first=owner_first,
        owner_last=owner_last,
        owner_confidence=owner_conf,
        owner_title=owner_title,
        was_scraped_direct=scraped,
        was_found_via_search=search,
        was_generated_from_pattern=generated,
        pattern_triangulated=email_matches_pattern,
        pattern_confidence=pattern_confidence,
        pattern_evidence_count=pattern_evidence_count,
        nb_valid=nb_valid,
        nb_invalid=nb_invalid,
        nb_catchall=nb_catchall,
        nb_unknown=nb_unknown,
        smtp_valid=bool(business.get("smtp_valid")),
        smtp_catchall=bool(business.get("smtp_catchall")),
        is_catchall_domain=bool(
            business.get("is_catchall_domain")
            or business.get("email_status") in ("catchall", "risky")
        ),
        owner_last_name_in_business=last_in_business,
    )


def compute_lead_quality_score(business: dict) -> dict:
    """
    Return {"score": int, "breakdown": {...}, "tier": "A".."F"}.
    Identity-heavy: rating, review count, and website presence NOT factors
    (we pre-filter to sites with websites; rating/reviews are selection
    criteria, not outreach-quality signals).
    """
    if (business.get("confidence") or "").lower() == "skip":
        return {
            "score": 0,
            "breakdown": {
                "source_evidence": 0, "triangulation": 0,
                "verification": 0, "owner_context": 0, "synergy": 0,
            },
            "tier": "F",
        }

    inputs = _business_dict_to_inputs(business)
    if inputs is None:
        return {
            "score": 0,
            "breakdown": {
                "source_evidence": 0, "triangulation": 0,
                "verification": 0, "owner_context": 0, "synergy": 0,
            },
            "tier": "F",
        }

    scored = score_email_candidate(inputs)
    return {
        "score": scored.score,
        "breakdown": scored.components,
        "tier": scored.grade,
        "specificity": scored.specificity.value,
        "is_catchall": scored.is_catchall,
        "is_triangulated": scored.is_triangulated,
        "requires_manual_review": scored.requires_manual_review,
    }


def rank_businesses(businesses: list, top_n: Optional[int] = None) -> list:
    for b in businesses:
        s = compute_lead_quality_score(b)
        b["lead_quality_score"] = s["score"]
        b["lead_tier"] = s["tier"]
        b["lead_score_breakdown"] = s["breakdown"]
    ranked = sorted(businesses, key=lambda b: -b.get("lead_quality_score", 0))
    if top_n:
        return ranked[:top_n]
    return ranked
