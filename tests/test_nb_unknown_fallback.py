"""
Tests for the NB-unknown rescue path: 2-attempt retry with backoff +
SMTP-probe fallback. Triggered by user reports of 48-54% NB-unknown
rate on medspa / vet vertical scrapes (search 58 + 59).

Root cause: small-biz domains (M365, Workspace, shared hosting) often
greylist or tarpit NB's verification probes. Greylisting clears within
~30 mins; SMTP RCPT TO often succeeds where NB's verification didn't.
"""
from src.volume_mode.ranking import (
    Candidate, confidence_tier,
    TIER_SCRAPED, TIER_REVIEW, TIER_VERIFIED, TIER_GUESS,
)


def test_smtp_confirmed_promotes_to_scraped_tier():
    """When NB stays unknown but SMTP probe gets 250 OK, the row
    should NOT stay in review — it has positive evidence."""
    c = Candidate(email="paula@firm.com", bucket="d",
                   pattern="{first}", nb_result="smtp_confirmed")
    assert confidence_tier(c) == TIER_SCRAPED


def test_nb_unknown_still_routes_to_review():
    c = Candidate(email="paula@firm.com", bucket="d",
                   pattern="{first}", nb_result="unknown")
    assert confidence_tier(c) == TIER_REVIEW


def test_nb_valid_still_routes_to_verified():
    c = Candidate(email="paula@firm.com", bucket="d",
                   pattern="{first}", nb_result="valid")
    assert confidence_tier(c) == TIER_VERIFIED


def test_smtp_confirmed_with_bucket_d_still_scraped():
    """Even from bucket d (industry prior), smtp_confirmed promotes
    above the default volume_guess tier."""
    c = Candidate(email="paula@firm.com", bucket="d",
                   pattern="{f}{last}", nb_result="smtp_confirmed")
    assert confidence_tier(c) == TIER_SCRAPED  # not GUESS


def test_smtp_confirmed_unaffected_by_cms_nudge():
    """CMS catchall nudge only fires on nb_result='catchall' — should
    NOT downgrade smtp_confirmed."""
    c = Candidate(email="paula@firm.com", bucket="d",
                   pattern="{f}{last}", nb_result="smtp_confirmed")
    assert confidence_tier(c, cms_catchall_hint="review") == TIER_SCRAPED
