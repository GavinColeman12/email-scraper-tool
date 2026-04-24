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

def test_extended_patterns_capped_at_max_candidates():
    """Default cap is 3 — we never return more than that."""
    pats = _extended_patterns("paula", "wyatt", "firm.com")
    assert len(pats) == 3, f"expected 3, got {len(pats)}: {pats}"


def test_extended_patterns_caller_can_raise_cap():
    pats = _extended_patterns(
        "paula", "wyatt", "firm.com", max_candidates=10,
    )
    assert len(pats) > 3


def test_extended_patterns_hardcoded_non_medical_order():
    """No learned data → hardcoded empirical order: flast, first, first.last.
    flast is the global #1 at 44% hit rate (learned from 96 samples)."""
    pats = _extended_patterns("paula", "wyatt", "firm.com",
                                vertical="law firm", max_candidates=3)
    emails = [e for _, e in pats]
    assert emails[0] == "pwyatt@firm.com"      # flast
    assert emails[1] == "paula@firm.com"        # first
    assert emails[2] == "paula.wyatt@firm.com"  # first.last


def test_extended_patterns_hardcoded_dental_order():
    """Dental hardcoded fallback: drlast, flast, first.last — matches
    the 15-sample learned distribution."""
    pats = _extended_patterns(
        "keenan", "fischman", "firm.com", vertical="dentist",
        max_candidates=3,
    )
    emails = [e for _, e in pats]
    assert emails[0] == "drfischman@firm.com"        # drlast
    assert emails[1] == "kfischman@firm.com"         # flast
    assert emails[2] == "keenan.fischman@firm.com"   # first.last


def test_extended_patterns_respects_learned_order():
    """When learned_order is provided, it overrides the hardcoded
    order. Caller in production passes what compute_learned_priors
    returned for this vertical."""
    pats = _extended_patterns(
        "paula", "wyatt", "firm.com",
        vertical="law firm", max_candidates=3,
        learned_order=["first.last", "first_last", "last.first"],
    )
    emails = [e for _, e in pats]
    assert emails[0] == "paula.wyatt@firm.com"
    assert emails[1] == "paula_wyatt@firm.com"
    assert emails[2] == "wyatt.paula@firm.com"


def test_extended_patterns_learned_order_fills_with_hardcoded():
    """Learned list has only 2 entries; slot 3 fills from hardcoded."""
    pats = _extended_patterns(
        "paula", "wyatt", "firm.com",
        vertical="law firm", max_candidates=3,
        learned_order=["first_last", "last.first"],
    )
    emails = [e for _, e in pats]
    assert emails[0] == "paula_wyatt@firm.com"
    assert emails[1] == "wyatt.paula@firm.com"
    # Slot 3: hardcoded top-3 minus what's already in learned
    # Hardcoded non-medical: flast, first, first.last → first not in
    # learned → pwyatt@firm.com (flast) fills slot 3
    assert emails[2] == "pwyatt@firm.com"


def test_extended_patterns_non_medical_skips_dr():
    pats = _extended_patterns(
        "paula", "wyatt", "firm.com", vertical="law firm",
        max_candidates=10,
    )
    emails = [e for _, e in pats]
    assert not any("dr.wyatt" in e for e in emails)


def test_extended_patterns_dedup():
    """Patterns that resolve to the same email should appear only once."""
    pats = _extended_patterns(
        "pat", "lee", "firm.com", vertical="dental", max_candidates=20,
    )
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
    """NB stays unknown on retry → fall through to pattern synthesis,
    bounded by the 3-call per-row budget."""
    biz = {
        "id": 1,
        "primary_email": "paula.wyatt@firm.com",
        "neverbounce_result": "unknown",
        "contact_name": "Paula Wyatt",
        "business_type": "Law firm",
    }
    # Retry returns unknown; first pattern invalid; second valid.
    # With budget=$0.009 (3 calls), we spend: 1 retry + 2 patterns.
    nb_fn = MagicMock(side_effect=[
        {"result": "unknown"},       # retry primary
        {"result": "invalid"},       # first pattern
        {"result": "valid"},         # second pattern wins
    ])
    result = rescue_review_row(biz, nb_verify_fn=nb_fn)
    assert result.status == "upgraded"
    assert result.new_nb_result == "valid"
    assert result.cost_usd == 3 * COST_NB_CALL


def test_rescue_stops_at_per_row_budget():
    """Default budget ($0.009) = 3 NB calls. Must not exceed that
    even if more patterns are available."""
    biz = {
        "id": 1,
        "primary_email": "manager@firm.com",
        "neverbounce_result": "valid",  # scraped shared inbox
        "contact_name": "Paula Wyatt",
        "business_type": "Dental clinic",
    }
    # Every NB call returns invalid — rescue must still stop at 3
    nb_fn = MagicMock(return_value={"result": "invalid"})
    result = rescue_review_row(biz, nb_verify_fn=nb_fn)
    assert result.status == "exhausted"
    assert nb_fn.call_count <= 3, (
        f"must not exceed 3 NB calls per row, made {nb_fn.call_count}"
    )


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


def test_apply_rescue_upgrade_writes_only_rescue_fields():
    """
    apply_rescue_upgrade() must UPDATE only the 5 rescue fields
    (primary_email, neverbounce_result, email_safe_to_send,
    confidence, scraped_at). Regression for the bug where a partial
    rescue persist via update_business_emails() was wiping
    contact_name, evidence_trail, triangulation fields, etc.

    Verified by asserting the exact SQL statement executed — we
    inspect the UPDATE's SET clause and confirm it doesn't include
    any evidence-trail columns.
    """
    from unittest.mock import patch, MagicMock
    from src import storage

    # Mock connect + cursor so we capture the SQL without hitting a DB
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    with patch.object(storage, "_connect", return_value=mock_conn), \
         patch.object(storage, "_cursor", return_value=mock_cur), \
         patch.object(storage, "init_db"):
        storage.apply_rescue_upgrade(
            business_id=42,
            new_email="paula.wyatt@wyattdental.com",
            new_nb_result="valid",
            confidence="high",
        )

    # execute was called once with the UPDATE
    assert mock_cur.execute.called
    sql, params = mock_cur.execute.call_args[0]

    # The SET clause must touch ONLY the five rescue fields.
    # Anything in the SET clause that's an evidence field is a bug.
    set_clause = sql.split("SET", 1)[1].split("WHERE", 1)[0].lower()
    allowed = {
        "primary_email", "neverbounce_result", "email_safe_to_send",
        "confidence", "scraped_at",
    }
    forbidden = {
        "contact_name", "contact_title", "email_source", "reasoning",
        "synthesizer", "professional_ids", "triangulation_pattern",
        "triangulation_confidence", "triangulation_method",
        "scraped_emails_json", "constructed_emails_json",
        "lead_quality_score", "lead_tier", "business_name",
    }
    for col in forbidden:
        assert col not in set_clause, \
            f"apply_rescue_upgrade() must NOT update {col!r} — would wipe evidence"
    for col in allowed:
        assert col in set_clause, \
            f"apply_rescue_upgrade() must update {col!r}"

    # Params — new_email first, NB result second, safe-to-send=1 (valid)
    assert params[0] == "paula.wyatt@wyattdental.com"
    assert params[1] == "valid"
    assert params[2] == 1  # safe_to_send=True because NB=valid
    assert params[3] == "high"
    assert params[-1] == 42  # business_id at end


def test_apply_rescue_upgrade_safe_to_send_false_on_non_valid_nb():
    """If the rescue winds up with non-valid NB (e.g. catchall), we
    still overwrite the email but email_safe_to_send MUST be 0 —
    we don't want rescue to upgrade a catchall pick to send-safe."""
    from unittest.mock import patch, MagicMock
    from src import storage

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    with patch.object(storage, "_connect", return_value=mock_conn), \
         patch.object(storage, "_cursor", return_value=mock_cur), \
         patch.object(storage, "init_db"):
        storage.apply_rescue_upgrade(
            business_id=1, new_email="x@y.com",
            new_nb_result="catchall", confidence="medium",
        )
    params = mock_cur.execute.call_args[0][1]
    assert params[1] == "catchall"
    assert params[2] == 0  # catchall is NOT safe_to_send


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
