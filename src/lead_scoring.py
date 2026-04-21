"""
Lead quality scoring — v3 (2026-04-21).

Transparent, identity-heavy formula:

   Total = Email Verifiability (40) + Decision Maker (40)
         + Review Count (15) + Google Rating (5)
   Max 100.
   Tiers: A=80+, B=65+, C=50+, D=35+, F<35

Design rules (explicit product decisions):
  - Email verifiability is the anchor. NB-valid on a DM email is the
    strongest signal we can produce; NB-valid on a random scraped
    person or NB-untested guess is worth noticeably less.
  - Decision-maker identity is equally weighted. A confirmed DM
    (name + executive title + last-name-matches-business + LinkedIn
    source) is as valuable as a verified email — a verified email
    for the wrong person is worth less than a verified email for the
    right person.
  - Website presence is NOT a component. Every business reaches this
    scorer via the crawl path, so it's a given.
  - Google rating and review count are selection signals, not outreach
    quality. Combined they cap at 20 (was 35); rating drops to 5.

Public surface preserved:
  - compute_lead_quality_score(business) -> {score, breakdown, tier}
  - rank_businesses(list, top_n=None) -> list
"""
from __future__ import annotations

import json
from typing import Optional


# ── Executive titles that signal a real decision maker ──────────────
_EXECUTIVE_TITLES = {
    "founder", "co-founder", "cofounder", "ceo", "chief executive",
    "president", "owner", "principal", "partner", "managing partner",
    "senior partner", "named partner", "proprietor",
    "managing member", "managing director", "md",
    "director", "executive director",
    # Healthcare — the practice-owner is usually the DM
    "dds", "dmd", "doctor", "dentist", "physician",
}

# Legal / professional-services partner titles that also count as DMs
_SENIOR_ROLE_TITLES = {
    "senior associate", "of counsel", "partner", "equity partner",
}


# ═════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════

def _parse_professional_ids(business: dict) -> dict:
    raw = business.get("professional_ids") or {}
    if isinstance(raw, str):
        try:
            return json.loads(raw) or {}
        except Exception:
            return {}
    return raw if isinstance(raw, dict) else {}


def _split_name(full: str) -> tuple[str, str]:
    parts = (full or "").strip().split()
    if len(parts) >= 2:
        return parts[0], parts[-1]
    if len(parts) == 1:
        return parts[0], ""
    return "", ""


def _is_executive_title(title: str) -> bool:
    t = (title or "").lower().strip()
    if not t:
        return False
    if t in _EXECUTIVE_TITLES:
        return True
    # Token-level check: "Managing Partner" → ("managing", "partner")
    tokens = set(t.split())
    for exec_t in _EXECUTIVE_TITLES | _SENIOR_ROLE_TITLES:
        exec_tokens = set(exec_t.split())
        if exec_tokens.issubset(tokens):
            return True
    return False


def _last_name_in_business(last: str, business_name: str) -> bool:
    """True when the DM's last name appears as a word in the business name."""
    if not last or not business_name:
        return False
    last_l = last.lower().strip()
    if len(last_l) < 3:
        return False  # too short to reliably match
    biz_l = business_name.lower()
    # Tokenize — avoid the "Martin" in "Martinez" false-positive
    biz_tokens = set()
    import re as _re
    for tok in _re.findall(r"[a-z]+", biz_l):
        biz_tokens.add(tok)
    return last_l in biz_tokens


def _find_picked_candidate(business: dict, prof: dict) -> Optional[dict]:
    """Return the candidate_emails entry matching primary_email, if any."""
    email = (business.get("primary_email") or "").lower()
    if not email:
        return None
    for c in (prof.get("candidate_emails") or []):
        if (c.get("email") or "").lower() == email:
            return c
    return None


def _email_status_fallback(business: dict) -> str:
    """Get NB result from the email_status column when candidate data is missing."""
    return (business.get("email_status") or "").lower().strip()


# ═════════════════════════════════════════════════════════════════════
# Component scorers
# ═════════════════════════════════════════════════════════════════════

def _score_email_verifiability(business: dict, prof: dict) -> int:
    """
    Max 40.

    Bucket × NB result matrix. Bucket comes from volume_mode's
    candidate_emails[].bucket; if missing (triangulation rows don't
    have it), we fall back to a simple heuristic based on email_source.
    """
    email = (business.get("primary_email") or "").strip()
    if not email:
        return 0

    picked = _find_picked_candidate(business, prof)
    nb_result = ""
    bucket = ""
    if picked:
        nb_result = (picked.get("nb_result") or "").lower()
        bucket = (picked.get("bucket") or "").lower()
    if not nb_result:
        nb_result = _email_status_fallback(business)

    if nb_result == "invalid":
        return 0

    # Infer bucket from email_source for non-volume-mode rows
    if not bucket:
        src = (business.get("email_source") or "").lower()
        if "triangulated" in src or "detected_pattern" in src:
            bucket = "b"
        elif "scraped" in src and "decision maker" in src:
            bucket = "a"
        elif "scraped" in src:
            bucket = "c"
        elif "industry prior" in src or "industry_prior" in src:
            bucket = "d"
        elif "fallback" in src:
            bucket = "e"
        else:
            bucket = "c"  # conservative default

    # Scoring matrix — rewards DM-matching buckets + NB verification
    matrix = {
        # (bucket, nb_result) -> points
        ("a", "valid"):    40,  # scraped DM email, verified
        ("b", "valid"):    38,  # triangulated DM pattern, verified
        ("d", "valid"):    36,  # industry-prior DM, verified — still a win
        ("c", "valid"):    30,  # scraped non-DM person, verified
        ("e", "valid"):    26,  # universal fallback, verified
        ("a", "catchall"): 24,
        ("b", "catchall"): 22,
        ("d", "catchall"): 20,
        ("c", "catchall"): 18,
        ("e", "catchall"): 15,
        ("a", "unknown"):  20,
        ("b", "unknown"):  18,
        ("d", "unknown"):  16,
        ("c", "unknown"):  14,
        ("e", "unknown"):  12,
        ("a", ""):         18,  # not tested
        ("b", ""):         16,
        ("d", ""):         12,  # untested industry-prior = weakest
        ("c", ""):         14,
        ("e", ""):         10,
    }
    return matrix.get((bucket, nb_result), 10)


def _score_decision_maker(business: dict, prof: dict) -> int:
    """
    Max 40. Additive signals, capped.

      +10  DM has first + last name
      +10  Title is executive (Founder, CEO, Owner, Managing Partner, ...)
      +10  Last name overlaps the business name
      +10  Source includes LinkedIn (professional identity confirmed)
      + 5  Source is multi-agent (cross-verified, e.g. "website + linkedin")
    """
    contact_name = (business.get("contact_name") or "").strip()
    if not contact_name:
        return 0

    first, last = _split_name(contact_name)
    title = business.get("contact_title") or ""
    business_name = business.get("business_name") or ""
    dm_info = (prof.get("decision_maker") or {}) if prof else {}
    source = (dm_info.get("source") or "").lower()

    score = 0
    # Base: we have a plausible name
    if first and last:
        score += 10
    elif first:
        score += 4  # partial credit for first-name-only

    # Title qualifies as executive
    if _is_executive_title(title):
        score += 10

    # Last name matches business name (strong DM signal for SMBs —
    # Weaver → Weaver Law, Gunnell → Gunnell Law, Ludlum → Ludlum Law)
    if _last_name_in_business(last, business_name):
        score += 10

    # LinkedIn source — confirmed professional identity
    if "linkedin" in source:
        score += 10

    # Multi-source cross-verification
    if "+" in source or source.count(",") > 0:
        score += 5

    return min(score, 40)


def _score_review_count(business: dict) -> int:
    """Max 15. Social-proof signal from Google Maps."""
    n = int(business.get("review_count") or 0)
    if n >= 500:
        return 15
    if n >= 200:
        return 13
    if n >= 100:
        return 11
    if n >= 50:
        return 8
    if n >= 10:
        return 5
    return 0


def _score_google_rating(business: dict) -> int:
    """Max 5. Rating-quality signal (de-weighted — 5% of total)."""
    try:
        r = float(business.get("rating") or 0)
    except (TypeError, ValueError):
        return 0
    if r >= 4.8:
        return 5
    if r >= 4.5:
        return 4
    if r >= 4.0:
        return 3
    if r >= 3.5:
        return 2
    return 0


# ═════════════════════════════════════════════════════════════════════
# Public API
# ═════════════════════════════════════════════════════════════════════

def compute_lead_quality_score(business: dict) -> dict:
    """
    Return {"score": int, "breakdown": {...}, "tier": "A".."F"}.

    Transparent formula:
      email_verifiability (40) + decision_maker (40)
      + review_count (15) + google_rating (5) = max 100
    """
    # SKIP confidence = hard reject regardless of other signals
    if (business.get("confidence") or "").lower() == "skip":
        return {
            "score": 0,
            "tier": "F",
            "breakdown": {
                "email_verifiability": 0,
                "decision_maker": 0,
                "review_count": 0,
                "google_rating": 0,
            },
        }

    prof = _parse_professional_ids(business)

    ev = _score_email_verifiability(business, prof)
    dm = _score_decision_maker(business, prof)
    rc = _score_review_count(business)
    gr = _score_google_rating(business)
    total = ev + dm + rc + gr

    if total >= 80:
        tier = "A"
    elif total >= 65:
        tier = "B"
    elif total >= 50:
        tier = "C"
    elif total >= 35:
        tier = "D"
    else:
        tier = "F"

    return {
        "score": total,
        "tier": tier,
        "breakdown": {
            "email_verifiability": ev,
            "decision_maker": dm,
            "review_count": rc,
            "google_rating": gr,
        },
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


# ── Backward-compat shim for decision_log.py ──
# The old scorer returned a rich structure that decision_log.py imports.
# Provide a dummy _business_dict_to_inputs that returns None — the
# scoring block in decision_log will skip the deep-path branch.
def _business_dict_to_inputs(business: dict):
    return None
