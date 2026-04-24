"""
Tests for src/review_rescue — the "try harder before giving up" layer
that retries NB on unknown rows and builds additional DM patterns for
name-mismatch rows.
"""
from unittest.mock import MagicMock

import pytest

from src.review_rescue import (
    rescue_review_row, bulk_rescue, _extended_patterns,
    RescueResult, COST_NB_CALL,
)


# ──────────────────────────────────────────────────────────────────────
# Pattern generator
# ──────────────────────────────────────────────────────────────────────

def test_extended_patterns_includes_separator_variants():
    pats = _extended_patterns("paula", "wyatt", "firm.com")
    emails = [e for _, e in pats]
    assert "paula_wyatt@firm.com" in emails
    assert "paula-wyatt@firm.com" in emails


def test_extended_patterns_includes_last_first():
    pats = _extended_patterns("paula", "wyatt", "firm.com")
    emails = [e for _, e in pats]
    assert "wyatt.paula@firm.com" in emails


def test_extended_patterns_dental_includes_dr_prefix():
    pats = _extended_patterns(
        "keenan", "fischman", "firm.com",
        vertical="dentist",
    )
    emails = [e for _, e in pats]
    assert "dr.fischman@firm.com" in emails
    assert any("doctorfischman" in e for e in emails)


def test_extended_patterns_non_medical_skips_dr():
    pats = _extended_patterns(
        "paula", "wyatt", "firm.com", vertical="law firm",
    )
    emails = [e for _, e in pats]
    assert not any("dr.wyatt" in e for e in emails)


def test_extended_patterns_dedup():
    """Patterns that resolve to the same email should appear only once."""
    pats = _extended_patterns("pat", "lee", "firm.com", vertical="dental")
    emails = [e for _, e in pats]
    assert len(emails) == len(set(emails))


def test_extended_patterns_empty_inputs():
    assert _extended_patterns("", "", "") == []
    assert _extended_patterns("paula", "wyatt", "") == []


# ──────────────────────────────────────────────────────────────────────
# Strategy 1: NB unknown → re-verify
# ──────────────────────────────────────────────────────────────────────

def test_rescue_nb_unknown_retry_succeeds():
    """NB-unknown row re-verifies as valid → upgraded, no pattern work needed."""
    biz = {
        "id": 1,
        "primary_email": "paula.wyatt@firm.com",
        "neverbounce_result": "unknown",
        "contact_name": "Paula Wyatt",
        "business_type": "Law firm",
    }
    nb_fn = MagicMock(return_value={"result": "valid", "safe_to_send": True})
    result = rescue_review_row(biz, nb_verify_fn=nb_fn)
    assert result.status == "upgraded"
    assert result.new_email == "paula.wyatt@firm.com"
    assert result.new_nb_result == "valid"
    assert result.cost_usd == COST_NB_CALL
    nb_fn.assert_called_once_with("paula.wyatt@firm.com")


def test_rescue_nb_unknown_retry_still_unknown_tries_patterns():
    """NB stays unknown on retry → fall through to pattern synthesis."""
    biz = {
        "id": 1,
        "primary_email": "paula.wyatt@firm.com",
        "neverbounce_result": "unknown",
        "contact_name": "Paula Wyatt",
        "business_type": "Law firm",
    }
    # Retry returns unknown; then pattern first_last returns valid
    nb_fn = MagicMock(side_effect=[
        {"result": "unknown"},       # retry primary
        {"result": "invalid"},       # first_last variant
        {"result": "invalid"},       # first-last variant
        {"result": "valid"},         # wyatt.paula@firm.com (last.first)
    ])
    result = rescue_review_row(biz, nb_verify_fn=nb_fn)
    assert result.status == "upgraded"
    assert result.new_nb_result == "valid"
    assert result.cost_usd == 4 * COST_NB_CALL


# ──────────────────────────────────────────────────────────────────────
# Strategy 2: Name-mismatch → try more patterns
# ──────────────────────────────────────────────────────────────────────

def test_rescue_finds_valid_on_alternative_pattern():
    """hba@emergencydental.com + Diana Giblette → rescue tries
    diana_giblette, diana-giblette, giblette.diana, giblette, etc.
    When one returns valid, we upgrade."""
    biz = {
        "id": 1,
        "primary_email": "hba@emergencydental.com",
        "neverbounce_result": "valid",  # scraped shared inbox
        "contact_name": "Diana Giblette",
        "business_type": "Dental clinic",
    }
    # first_last valid on first try
    nb_fn = MagicMock(return_value={"result": "valid"})
    result = rescue_review_row(biz, nb_verify_fn=nb_fn)
    # NB-valid row doesn't trigger the retry path, so we go straight
    # to pattern synthesis
    assert result.status == "upgraded"
    assert result.new_email != "hba@emergencydental.com"


def test_rescue_exhausted_after_all_patterns_invalid():
    biz = {
        "id": 1,
        "primary_email": "manager@firm.com",
        "neverbounce_result": "valid",
        "contact_name": "Michel Kuri",
        "business_type": "Dental",
    }
    # Every NB call returns invalid
    nb_fn = MagicMock(return_value={"result": "invalid"})
    result = rescue_review_row(biz, nb_verify_fn=nb_fn)
    assert result.status == "exhausted"
    assert result.new_email is None
    assert result.cost_usd > 0
    assert len(result.attempts) >= 1


def test_rescue_respects_budget_cap():
    """Per-row budget of $0.003 = 1 NB call max."""
    biz = {
        "id": 1,
        "primary_email": "patientbilling@firm.com",
        "neverbounce_result": "valid",
        "contact_name": "William Cote",
        "business_type": "Dental clinic",
    }
    nb_fn = MagicMock(return_value={"result": "invalid"})
    result = rescue_review_row(biz, nb_verify_fn=nb_fn, budget_usd=0.003)
    # Should stop after 1 NB call (budget = 1 × $0.003)
    assert result.status == "exhausted"
    assert result.cost_usd == COST_NB_CALL
    assert nb_fn.call_count == 1


# ──────────────────────────────────────────────────────────────────────
# Skipping
# ──────────────────────────────────────────────────────────────────────

def test_rescue_skips_without_domain():
    biz = {
        "id": 1,
        "primary_email": "",
        "website": "",
        "contact_name": "Paula Wyatt",
    }
    nb_fn = MagicMock()
    result = rescue_review_row(biz, nb_verify_fn=nb_fn)
    assert result.status == "skipped"
    assert not nb_fn.called


def test_rescue_derives_domain_from_website_when_no_email():
    biz = {
        "id": 1,
        "primary_email": "",
        "website": "https://wyattlawfirm.com/about",
        "contact_name": "Paula Wyatt",
        "business_type": "Law firm",
    }
    nb_fn = MagicMock(return_value={"result": "valid"})
    result = rescue_review_row(biz, nb_verify_fn=nb_fn)
    # Should produce a candidate email at wyattlawfirm.com
    assert result.status == "upgraded"
    assert result.new_email and "wyattlawfirm.com" in result.new_email


# ──────────────────────────────────────────────────────────────────────
# Bulk rescue
# ──────────────────────────────────────────────────────────────────────

def test_bulk_rescue_respects_total_budget():
    """Global budget cap stops iteration before exhausting all rows."""
    businesses = [
        {"id": i, "primary_email": f"test{i}@firm.com",
         "neverbounce_result": "unknown", "contact_name": "Paula Wyatt",
         "business_type": "Law firm"}
        for i in range(100)
    ]
    nb_fn = MagicMock(return_value={"result": "invalid"})
    # $0.006 budget = ~1 row (each row can spend up to $0.018; we clamp
    # per-row to remaining global, so row 1 spends $0.006 and row 2
    # is blocked).
    summary = bulk_rescue(businesses, total_budget_usd=0.006,
                           nb_verify_fn=nb_fn)
    assert summary["stopped_early"]
    assert summary["total_cost_usd"] <= 0.006 + 1e-9


def test_bulk_rescue_buckets_results():
    businesses = [
        {"id": 1, "primary_email": "ok@firm.com",
         "neverbounce_result": "unknown", "contact_name": "Paula Wyatt",
         "business_type": "Law firm"},  # upgraded
        {"id": 2, "primary_email": "", "website": "",
         "contact_name": "X"},  # skipped (no domain)
    ]
    # Retry of ok@firm.com returns valid
    nb_fn = MagicMock(return_value={"result": "valid"})
    summary = bulk_rescue(businesses, total_budget_usd=5.0, nb_verify_fn=nb_fn)
    assert len(summary["upgraded"]) == 1
    assert len(summary["skipped"]) == 1
    assert len(summary["exhausted"]) == 0
