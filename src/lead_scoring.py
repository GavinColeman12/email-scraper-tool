"""
Lead quality scoring — produce a 0-100 composite score per business
so you can sort and pick the top N high-quality leads from a large batch.

Score components (weighted):
  40% — Email confidence (scraped > cross-verified > constructed > generic)
  20% — Business rating (Google Maps 0-5 stars)
  15% — Review count (more reviews = more established)
  10% — Website present
  15% — Decision maker identified (has name + title)
"""


# Confidence → base points (out of 40)
_EMAIL_CONFIDENCE_POINTS = {
    "high": 40,
    "medium": 25,
    "low": 10,
    "": 0,
    None: 0,
}

# Source → bonus points (caps the confidence score at 40 total)
_EMAIL_SOURCE_BONUS = {
    "scraped_mailto_or_regex": 0,          # already high
    "scraped_personal_email": 0,            # already high
    "team_page_verified_by_linkedin": 0,    # already high
    "linkedin_verified_by_website": 0,      # already high
    "team_page_decision_maker": 2,          # solid medium
    "constructed_from_linkedin": 2,
    "constructed_from_website_decision_maker": 1,
    "team_page_person": 0,
    "constructed_from_website_name": -3,    # weaker medium
    "generic_inbox": -5,                     # penalize generic
    "generic_fallback": -5,
}

# Verification status → adjustment
_VERIFY_ADJUSTMENT = {
    "valid": 0,           # no adjustment for passing
    "risky": -5,          # catch-all — uncertain
    "invalid": -40,       # kill the score
    "disposable": -40,
    "unknown": -2,
    "": 0,
    None: 0,
}


def _rating_points(rating) -> int:
    """Convert Google rating to 0-20 points."""
    try:
        r = float(rating or 0)
    except Exception:
        return 0
    if r >= 4.8:
        return 20
    if r >= 4.5:
        return 17
    if r >= 4.0:
        return 13
    if r >= 3.5:
        return 8
    if r >= 3.0:
        return 4
    return 0


def _review_count_points(count) -> int:
    """Convert review count to 0-15 points on a log-ish curve."""
    try:
        n = int(count or 0)
    except Exception:
        return 0
    if n >= 500:
        return 15
    if n >= 200:
        return 13
    if n >= 100:
        return 11
    if n >= 50:
        return 8
    if n >= 25:
        return 5
    if n >= 10:
        return 3
    if n > 0:
        return 1
    return 0


def _website_points(website) -> int:
    """10 pts if has a real website, 0 otherwise."""
    if not website:
        return 0
    w = str(website).lower().strip()
    if w in ("—", "-", "none", "null", ""):
        return 0
    if "facebook.com" in w or "instagram.com" in w or "linkedin.com" in w:
        # Social-only presence is weaker than a real domain
        return 4
    return 10


def _decision_maker_points(business: dict) -> int:
    """15 pts if both contact name AND title are present."""
    name = (business.get("contact_name") or "").strip()
    title = (business.get("contact_title") or "").strip()
    if name and title:
        return 15
    if name:
        return 10
    if title:
        return 4
    return 0


def compute_lead_quality_score(business: dict) -> dict:
    """
    Return a dict with the composite score + a breakdown explaining how
    each component contributed. UI can show the breakdown on hover.
    """
    # Email confidence component (0-40)
    confidence = business.get("confidence") or ""
    source = business.get("email_source") or ""
    verify_status = business.get("email_status") or ""

    email_pts = _EMAIL_CONFIDENCE_POINTS.get(confidence, 0)
    email_pts += _EMAIL_SOURCE_BONUS.get(source, 0)
    email_pts += _VERIFY_ADJUSTMENT.get(verify_status, 0)
    email_pts = max(0, min(40, email_pts))

    rating_pts = _rating_points(business.get("rating"))
    reviews_pts = _review_count_points(business.get("review_count"))
    website_pts = _website_points(business.get("website"))
    dm_pts = _decision_maker_points(business)

    total = email_pts + rating_pts + reviews_pts + website_pts + dm_pts

    return {
        "score": int(total),
        "breakdown": {
            "email_confidence": int(email_pts),    # /40
            "rating": int(rating_pts),              # /20
            "reviews": int(reviews_pts),            # /15
            "website": int(website_pts),            # /10
            "decision_maker": int(dm_pts),          # /15
        },
        "tier": _tier_from_score(int(total)),
    }


def _tier_from_score(score: int) -> str:
    """Qualitative tier for filtering / UI."""
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    if score >= 35:
        return "D"
    return "F"


def rank_businesses(businesses: list, top_n: int = None) -> list:
    """
    Return businesses sorted by lead_quality_score DESC.
    Mutates each business to add 'lead_quality_score' and 'lead_tier'.
    """
    for b in businesses:
        s = compute_lead_quality_score(b)
        b["lead_quality_score"] = s["score"]
        b["lead_tier"] = s["tier"]
        b["lead_score_breakdown"] = s["breakdown"]
    ranked = sorted(businesses, key=lambda b: -b.get("lead_quality_score", 0))
    if top_n:
        return ranked[:top_n]
    return ranked
