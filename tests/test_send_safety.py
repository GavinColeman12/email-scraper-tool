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


# ──────────────────────────────────────────────────────────────────────
# DM-name-match gate — the search-45 dental leaks
# ──────────────────────────────────────────────────────────────────────

from src.send_safety import _local_contains_dm_name, mark_duplicate_emails


@pytest.mark.parametrize("email,first,last,expected", [
    # Direct name matches
    ("paula.wyatt@firm.com", "Paula", "Wyatt", True),
    ("pwyatt@firm.com", "Paula", "Wyatt", True),      # {f}{last}
    ("paulaw@firm.com", "Paula", "Wyatt", True),      # {first}{l}
    ("drwyatt@firm.com", "Paula", "Wyatt", True),     # dr{last} dental pattern
    # Nickname equivalents
    ("jeff.buhrman@firm.com", "Jeffrey", "Buhrman", True),  # Jeff ↔ Jeffrey
    ("mike.crow@firm.com", "Michael", "Crow", True),        # Mike ↔ Michael
    # Search-45 failures that name-match catches:
    ("hba@emergencydental.com", "Diana", "Giblette", False),
    ("civilrights@dtcc.edu", "David", "Wachsman", False),
    ("manager@dentistnearmeep.com", "Michel", "Kuri", False),
    ("patientbilling@mydrdental.com", "William", "Cote", False),
    # Short last names should not produce false positives
    ("abc@foo.com", "Sarah", "Lu", False),
    # Empty DM info → False (can't evaluate)
    ("anyone@foo.com", "", "", False),
])
def test_local_contains_dm_name(email, first, last, expected):
    assert _local_contains_dm_name(email, first, last) == expected


def test_safe_to_send_rejects_wrong_person_email():
    """Search 45, row 3: hba@emergencydental.com, DM=Diana Giblette.
    NB-valid + scraped from website but local doesn't contain Diana's
    name — classic shared-inbox or wrong-person trap."""
    biz = _biz(
        primary_email="hba@emergencydental.com",
        contact_name="Diana Giblette",
    )
    safe, reasons = is_safe_to_send(biz)
    assert not safe
    assert any("doesn't match dm name" in r.lower() for r in reasons)


def test_safe_to_send_rejects_civilrights_edu():
    """Search 45, row 16: civilrights@dtcc.edu with DM David Wachsman."""
    biz = _biz(
        primary_email="civilrights@dtcc.edu",
        contact_name="David Wachsman",
    )
    safe, reasons = is_safe_to_send(biz)
    assert not safe


def test_safe_to_send_accepts_dr_prefix_dental_pattern():
    """Search 45, row 1: drfischman@nebraskadentalcenter.com with DM
    Keenan Fischman. "dr{last}" is a common dental pattern and should
    pass the name-match gate."""
    biz = _biz(
        primary_email="drfischman@nebraskadentalcenter.com",
        contact_name="Keenan Fischman",
    )
    safe, reasons = is_safe_to_send(biz)
    assert safe, f"dr{{last}} pattern should pass: {reasons}"


def test_safe_to_send_accepts_nickname_match():
    """Jeff ↔ Jeffrey should count as a match."""
    biz = _biz(
        primary_email="jeff.buhrman@firm.com",
        contact_name="Jeffrey Buhrman",
    )
    safe, reasons = is_safe_to_send(biz)
    assert safe, f"nickname should pass: {reasons}"


def test_classify_review_on_name_mismatch():
    """Name-mismatch rows go to review, not skip — the mailbox may
    still deliver, but needs human check."""
    biz = _biz(
        primary_email="hba@emergencydental.com",
        contact_name="Diana Giblette",
    )
    assert classify_for_send(biz) == "review"


def test_permissive_mode_skips_name_match():
    """Permissive should relax name-match along with rating/freshness."""
    biz = _biz(
        primary_email="hba@emergencydental.com",
        contact_name="Diana Giblette",
    )
    safe, _ = is_safe_to_send(biz, permissive=True)
    assert safe


# ──────────────────────────────────────────────────────────────────────
# Duplicate email detection
# ──────────────────────────────────────────────────────────────────────

def test_mark_duplicates_identifies_second_occurrence():
    """Search 45, rows 14+15: mohammad.spouh@aspendental.com on two
    franchise locations. Second occurrence should be flagged."""
    rows = [
        {"id": 1, "primary_email": "mohammad.spouh@aspendental.com"},
        {"id": 2, "primary_email": "unique@firm.com"},
        {"id": 3, "primary_email": "mohammad.spouh@aspendental.com"},
        {"id": 4, "primary_email": "mohammad.spouh@aspendental.com"},
    ]
    dup = mark_duplicate_emails(rows)
    assert dup[1] == 0   # first occurrence — safe
    assert dup[2] == 0   # unrelated email
    assert dup[3] == 1   # second occurrence
    assert dup[4] == 2   # third occurrence


def test_mark_duplicates_case_insensitive():
    rows = [
        {"id": 1, "primary_email": "Foo@Bar.Com"},
        {"id": 2, "primary_email": "foo@bar.com"},
    ]
    dup = mark_duplicate_emails(rows)
    assert dup[1] == 0
    assert dup[2] == 1


def test_mark_duplicates_skips_empty():
    rows = [
        {"id": 1, "primary_email": ""},
        {"id": 2, "primary_email": None},
        {"id": 3, "primary_email": "real@firm.com"},
    ]
    dup = mark_duplicate_emails(rows)
    # Empty emails aren't tracked — missing from dict
    assert 1 not in dup
    assert 2 not in dup
    assert dup[3] == 0


# ──────────────────────────────────────────────────────────────────────
# Stopword expansion (search 45)
# ──────────────────────────────────────────────────────────────────────

from src.volume_mode.stopwords import is_generic


@pytest.mark.parametrize("local", [
    # Healthcare-specific from search 45
    "manager", "patientbilling", "patientsupport", "patients",
    "patientcare", "patientservice",
    "civilrights", "compliance",
    # Managerial variants
    "generalmanager", "practicemanager", "officemanager",
    "regionalmanager",
    # Additional medical ops
    "insurance", "claims", "records", "medicalrecords",
    "nursing",
])
def test_search45_generic_locals_rejected(local):
    assert is_generic(local), f"{local!r} should be generic"
