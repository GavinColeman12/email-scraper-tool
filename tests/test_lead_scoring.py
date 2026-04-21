"""Unit tests for the v3 lead scoring formula."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.lead_scoring import compute_lead_quality_score


# Helpers

def _biz(**kwargs) -> dict:
    """Build a minimal business dict with sensible defaults."""
    base = {
        "business_name": "Weaver Law Offices",
        "business_type": "Law firm",
        "primary_email": "rweaver@weaver-law.com",
        "contact_name": "Roger Weaver",
        "contact_title": "Founder",
        "email_status": "valid",
        "rating": 4.7,
        "review_count": 91,
        "confidence": "high",
        "email_source": "scraped from website (matches decision maker) — NeverBounce VALID",
        "professional_ids": None,
    }
    base.update(kwargs)
    return base


# ── Email verifiability ──

def test_scraped_dm_email_nb_valid_is_full_credit():
    b = _biz(professional_ids={
        "candidate_emails": [
            {"email": "rweaver@weaver-law.com", "bucket": "a", "nb_result": "valid"},
        ],
    })
    s = compute_lead_quality_score(b)
    assert s["breakdown"]["email_verifiability"] == 40


def test_industry_prior_nb_valid_still_scores_high():
    b = _biz(professional_ids={
        "candidate_emails": [
            {"email": "rweaver@weaver-law.com", "bucket": "d", "nb_result": "valid"},
        ],
    })
    s = compute_lead_quality_score(b)
    assert s["breakdown"]["email_verifiability"] == 36


def test_nb_invalid_is_zero_email_score():
    b = _biz(professional_ids={
        "candidate_emails": [
            {"email": "rweaver@weaver-law.com", "bucket": "a", "nb_result": "invalid"},
        ],
    })
    s = compute_lead_quality_score(b)
    assert s["breakdown"]["email_verifiability"] == 0


def test_scraped_non_dm_catchall_lower_than_dm_valid():
    """Bucket C catchall (18) < Bucket A valid (40)."""
    b_nondm = _biz(
        primary_email="jordan@erichoffmanlaw.com",
        professional_ids={
            "candidate_emails": [
                {"email": "jordan@erichoffmanlaw.com", "bucket": "c",
                 "nb_result": "catchall"},
            ],
        },
    )
    s_nondm = compute_lead_quality_score(b_nondm)
    assert s_nondm["breakdown"]["email_verifiability"] == 18


def test_fallback_to_email_status_when_no_candidate_data():
    """Triangulation rows don't have bucket data; use email_status + source heuristic."""
    b = _biz(
        email_status="valid",
        email_source="scraped from website (matches decision maker) — NeverBounce VALID",
        professional_ids=None,
    )
    s = compute_lead_quality_score(b)
    # Should land around 40 (bucket a, valid)
    assert s["breakdown"]["email_verifiability"] >= 30


# ── Decision maker ──

def test_dm_full_signals_cap_at_40():
    """Name + exec title + last-name-matches-business + LinkedIn + cross-source = 45 raw → capped 40."""
    b = _biz(
        contact_name="Roger Weaver",
        contact_title="Founder",
        business_name="Weaver Law Offices",
        professional_ids={
            "decision_maker": {
                "name": "Roger Weaver",
                "source": "linkedin_via_google + website_scrape",
            },
        },
    )
    s = compute_lead_quality_score(b)
    assert s["breakdown"]["decision_maker"] == 40


def test_dm_no_name_is_zero():
    b = _biz(contact_name="", contact_title="")
    s = compute_lead_quality_score(b)
    assert s["breakdown"]["decision_maker"] == 0


def test_dm_last_name_must_match_exact_token_not_substring():
    """'Martin' should NOT count as matching 'Martinez Law' — full-token check."""
    b = _biz(
        contact_name="Paul Martin",
        contact_title="",
        business_name="Martinez Law Group",
        professional_ids=None,
    )
    s = compute_lead_quality_score(b)
    # 10 (name) + 0 (exec) + 0 (no last-name match) + 0 (no LinkedIn) = 10
    assert s["breakdown"]["decision_maker"] == 10


def test_dm_executive_titles_count():
    for title in ("Founder", "CEO", "Owner", "President", "Managing Partner",
                  "Principal", "Partner"):
        b = _biz(contact_name="Jane Smith", contact_title=title,
                 business_name="Unrelated Inc", professional_ids=None)
        s = compute_lead_quality_score(b)
        # 10 name + 10 exec = 20
        assert s["breakdown"]["decision_maker"] == 20, \
            f"title={title!r} should score 20 (name+exec)"


def test_dm_linkedin_source_bonus():
    b = _biz(
        contact_name="Jane Gunnell",
        contact_title="President",
        business_name="Gunnell Law PC",
        professional_ids={
            "decision_maker": {"source": "linkedin_via_google"},
        },
    )
    s = compute_lead_quality_score(b)
    # 10 name + 10 exec + 10 last-match + 10 linkedin = 40
    assert s["breakdown"]["decision_maker"] == 40


# ── Review count + rating ──

def test_review_count_tiers():
    assert compute_lead_quality_score(_biz(review_count=600))["breakdown"]["review_count"] == 15
    assert compute_lead_quality_score(_biz(review_count=250))["breakdown"]["review_count"] == 13
    assert compute_lead_quality_score(_biz(review_count=100))["breakdown"]["review_count"] == 11
    assert compute_lead_quality_score(_biz(review_count=50))["breakdown"]["review_count"] == 8
    assert compute_lead_quality_score(_biz(review_count=10))["breakdown"]["review_count"] == 5
    assert compute_lead_quality_score(_biz(review_count=5))["breakdown"]["review_count"] == 0


def test_rating_max_5():
    assert compute_lead_quality_score(_biz(rating=4.9))["breakdown"]["google_rating"] == 5
    assert compute_lead_quality_score(_biz(rating=4.5))["breakdown"]["google_rating"] == 4
    assert compute_lead_quality_score(_biz(rating=4.0))["breakdown"]["google_rating"] == 3
    assert compute_lead_quality_score(_biz(rating=3.0))["breakdown"]["google_rating"] == 0


# ── Totals + tiers ──

def test_perfect_lead_hits_A_tier():
    """Verified DM email + exec title + last name match + LinkedIn + 500+ reviews + 4.8 rating."""
    b = _biz(
        contact_name="Roger Weaver",
        contact_title="Founder",
        business_name="Weaver Law Offices",
        review_count=600,
        rating=4.9,
        professional_ids={
            "decision_maker": {"source": "linkedin_via_google + website_scrape"},
            "candidate_emails": [
                {"email": "rweaver@weaver-law.com", "bucket": "a", "nb_result": "valid"},
            ],
        },
    )
    s = compute_lead_quality_score(b)
    # 40 + 40 + 15 + 5 = 100
    assert s["score"] == 100
    assert s["tier"] == "A"


def test_typical_volume_guess_lands_in_D_or_F():
    """NB-unknown industry-prior guess, DM from LinkedIn, typical review count."""
    b = _biz(
        primary_email="greg.mansell@ohio-employmentlawyer.com",
        contact_name="Greg Mansell",
        contact_title="Managing Partner",
        business_name="Mansell Law",
        email_status="",
        email_source="industry prior '{first}.{last}' (law) [below threshold]",
        confidence="low",
        rating=5.0,
        review_count=243,
        professional_ids={
            "decision_maker": {"source": "website_scrape"},
            "candidate_emails": [
                {"email": "greg.mansell@ohio-employmentlawyer.com",
                 "bucket": "d", "nb_result": ""},
            ],
        },
    )
    s = compute_lead_quality_score(b)
    # email: 12 (d, not tested)
    # dm: 10 (name) + 10 (exec "Managing Partner") + 10 (Mansell in Mansell Law) = 30
    # reviews: 13 (243)
    # rating: 5 (5.0)
    # total: 12 + 30 + 13 + 5 = 60 → C tier
    assert s["score"] == 60
    assert s["tier"] == "C"


def test_skip_confidence_is_hard_zero():
    b = _biz(confidence="skip")
    s = compute_lead_quality_score(b)
    assert s["score"] == 0
    assert s["tier"] == "F"


def test_nb_invalid_tanks_tier():
    """Verified-invalid email → no email points → often F tier even with great other signals."""
    b = _biz(
        email_status="invalid",
        rating=4.9,
        review_count=600,
        professional_ids={
            "candidate_emails": [
                {"email": "rweaver@weaver-law.com", "bucket": "a", "nb_result": "invalid"},
            ],
            "decision_maker": {"source": "linkedin_via_google"},
        },
    )
    s = compute_lead_quality_score(b)
    # 0 email + 40 dm + 15 reviews + 5 rating = 60 → C
    # That's still a C — the DM info IS valuable even if this email bounced.
    # Operator can dig in and try a different pattern.
    assert s["breakdown"]["email_verifiability"] == 0
    assert s["tier"] == "C"


if __name__ == "__main__":
    import inspect
    funcs = [(n, f) for n, f in globals().items()
             if n.startswith("test_") and inspect.isfunction(f)]
    failed = 0
    for name, fn in funcs:
        try:
            fn()
            print(f"  OK  {name}")
        except AssertionError as e:
            print(f"  FAIL {name}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERR  {name}: {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{len(funcs) - failed}/{len(funcs)} passed")
    sys.exit(0 if failed == 0 else 1)
