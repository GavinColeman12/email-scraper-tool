"""
Unit tests for src/send_safety — the strict pre-send gate for <0.3%
bounce-rate targets.

Covers:
  - NB verdict gating (valid / catchall / unknown / invalid / missing)
  - Freshness gate (stale NB verdicts)
  - Domain bounce-history blocklist
  - Rating + review-count gates
  - permissive=True escape hatch
  - classify_for_send bucketing
  - Warmup schedule progression
"""
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from src.send_safety import (
    is_safe_to_send, classify_for_send,
    recommended_daily_cap, WARMUP_SCHEDULE,
    _parse_dt, _domain_of,
)


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def _biz(**overrides) -> dict:
    """Build a minimally-valid send-safe biz row, then override fields."""
    base = {
        "id": 1,
        "primary_email": "paula.wyatt@wyattlawfirm.com",
        "neverbounce_result": "valid",
        "scraped_at": datetime.utcnow() - timedelta(days=2),
        "rating": 4.8,
        "review_count": 120,
        "email_safe_to_send": True,
    }
    base.update(overrides)
    return base


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────

def test_valid_biz_passes_all_gates():
    safe, reasons = is_safe_to_send(_biz())
    assert safe, f"expected safe, got reasons={reasons}"
    assert reasons == []


# ──────────────────────────────────────────────────────────────────────
# NB verdict gating
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("nb,expected_fragment", [
    ("catchall", "catchall"),
    ("catch-all", "catchall"),
    ("unknown", "NB unknown"),
    ("invalid", "NB invalid"),
    ("", "NB untested"),
    (None, "NB untested"),
])
def test_nb_non_valid_rejected(nb, expected_fragment):
    safe, reasons = is_safe_to_send(_biz(neverbounce_result=nb))
    assert not safe
    assert any(expected_fragment.lower() in r.lower() for r in reasons), \
        f"expected {expected_fragment!r} in {reasons}"


# ──────────────────────────────────────────────────────────────────────
# Freshness gate
# ──────────────────────────────────────────────────────────────────────

def test_stale_nb_rejected_by_default():
    old = datetime.utcnow() - timedelta(days=30)
    safe, reasons = is_safe_to_send(_biz(scraped_at=old))
    assert not safe
    assert any("stale" in r.lower() for r in reasons)


def test_stale_nb_passes_with_permissive():
    old = datetime.utcnow() - timedelta(days=30)
    safe, reasons = is_safe_to_send(_biz(scraped_at=old), permissive=True)
    assert safe, f"permissive mode should accept stale: {reasons}"


def test_no_scraped_at_flagged_stale():
    safe, reasons = is_safe_to_send(_biz(scraped_at=None))
    assert not safe
    assert any("timestamp" in r.lower() for r in reasons)


# ──────────────────────────────────────────────────────────────────────
# Domain bounce history
# ──────────────────────────────────────────────────────────────────────

def test_domain_bounce_history_blocks_send():
    bounced = {"wyattlawfirm.com"}
    safe, reasons = is_safe_to_send(_biz(), domain_bounce_set=bounced)
    assert not safe
    assert any("prior bounces" in r.lower() for r in reasons)


def test_unrelated_domain_bounces_dont_block():
    bounced = {"someotherfirm.com"}
    safe, _ = is_safe_to_send(_biz(), domain_bounce_set=bounced)
    assert safe


# ──────────────────────────────────────────────────────────────────────
# Rating / review gates
# ──────────────────────────────────────────────────────────────────────

def test_low_rating_rejected():
    safe, reasons = is_safe_to_send(_biz(rating=2.0))
    assert not safe
    assert any("rating" in r.lower() for r in reasons)


def test_zero_reviews_rejected():
    safe, reasons = is_safe_to_send(_biz(review_count=0))
    assert not safe
    assert any("review" in r.lower() for r in reasons)


def test_low_rating_passes_with_permissive():
    safe, _ = is_safe_to_send(_biz(rating=2.0, review_count=0), permissive=True)
    assert safe


# ──────────────────────────────────────────────────────────────────────
# Pipeline safe_to_send flag
# ──────────────────────────────────────────────────────────────────────

def test_pipeline_safe_to_send_false_blocks():
    safe, reasons = is_safe_to_send(_biz(email_safe_to_send=False))
    assert not safe
    assert any("safe_to_send=false" in r.lower() for r in reasons)


def test_pipeline_safe_to_send_zero_sqlite_blocks():
    """SQLite stores False as 0 — verify the filter respects that."""
    safe, reasons = is_safe_to_send(_biz(email_safe_to_send=0))
    assert not safe


# ──────────────────────────────────────────────────────────────────────
# classify_for_send bucketing
# ──────────────────────────────────────────────────────────────────────

def test_classify_send_all_gates_pass():
    assert classify_for_send(_biz()) == "send"


def test_classify_skip_on_nb_invalid():
    assert classify_for_send(_biz(neverbounce_result="invalid")) == "skip"


def test_classify_skip_on_domain_bounced():
    result = classify_for_send(
        _biz(), domain_bounce_set={"wyattlawfirm.com"}
    )
    assert result == "skip"


def test_classify_reverify_on_stale_only():
    old = datetime.utcnow() - timedelta(days=30)
    assert classify_for_send(_biz(scraped_at=old)) == "reverify"


def test_classify_review_on_catchall():
    assert classify_for_send(_biz(neverbounce_result="catchall")) == "review"


# ──────────────────────────────────────────────────────────────────────
# Warmup schedule
# ──────────────────────────────────────────────────────────────────────

def test_warmup_brand_new_sender():
    result = recommended_daily_cap(None)
    assert result["cap"] == 50
    assert result["week"] == 0


def test_warmup_week_1():
    first = datetime.utcnow() - timedelta(days=3)
    result = recommended_daily_cap(first)
    assert result["cap"] == 50
    assert result["week"] == 1


def test_warmup_week_4():
    first = datetime.utcnow() - timedelta(days=22)
    result = recommended_daily_cap(first)
    # Week 4 should be at 400/day
    assert result["cap"] == 400


def test_warmup_full_volume_after_week_13():
    first = datetime.utcnow() - timedelta(days=120)
    result = recommended_daily_cap(first)
    assert result["cap"] == WARMUP_SCHEDULE[-1][1]
    assert result["stage"] == "full volume"
    assert result["next_bump_in_days"] is None


# ──────────────────────────────────────────────────────────────────────
# Parse-time tolerance
# ──────────────────────────────────────────────────────────────────────

def test_parse_dt_accepts_datetime_and_iso_string():
    dt = datetime(2026, 4, 1, 12, 0)
    assert _parse_dt(dt).year == 2026
    assert _parse_dt("2026-04-01T12:00:00").year == 2026
    assert _parse_dt("2026-04-01T12:00:00Z").year == 2026
    assert _parse_dt(None) is None
    assert _parse_dt("not a date") is None


def test_domain_of_basic():
    assert _domain_of("foo@bar.com") == "bar.com"
    assert _domain_of("Foo@Bar.COM") == "bar.com"
    assert _domain_of("not-an-email") == ""
    assert _domain_of("") == ""
