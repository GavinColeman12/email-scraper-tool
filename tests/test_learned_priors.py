"""
Tests for src/learned_priors — pattern classifier + aggregator that
learns hit rates per vertical from our own NB-valid history.
"""
import pytest

from src.learned_priors import (
    classify_pattern, _nb_verdict_of, _first_last_of,
)


# ──────────────────────────────────────────────────────────────────────
# Pattern classifier — identifies which pattern produced an email
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("email,first,last,expected", [
    ("paula.wyatt@firm.com", "Paula", "Wyatt", "first.last"),
    ("pwyatt@firm.com", "Paula", "Wyatt", "flast"),
    ("paulaw@firm.com", "Paula", "Wyatt", "firstl"),
    ("paula_wyatt@firm.com", "Paula", "Wyatt", "first_last"),
    ("paula-wyatt@firm.com", "Paula", "Wyatt", "first-last"),
    ("wyatt.paula@firm.com", "Paula", "Wyatt", "last.first"),
    ("wyattpaula@firm.com", "Paula", "Wyatt", "lastfirst"),
    ("paula@firm.com", "Paula", "Wyatt", "first"),
    ("wyatt@firm.com", "Paula", "Wyatt", "last"),
    ("drwyatt@firm.com", "Paula", "Wyatt", "drlast"),
    ("dr.wyatt@firm.com", "Paula", "Wyatt", "dr.last"),
    ("drpaula@firm.com", "Paula", "Wyatt", "drfirst"),
    ("doctorwyatt@firm.com", "Paula", "Wyatt", "doctorlast"),
    ("pw@firm.com", "Paula", "Wyatt", "fl"),
    # Shared inboxes / unknown patterns return None
    ("info@firm.com", "Paula", "Wyatt", None),
    ("manager@firm.com", "Paula", "Wyatt", None),
    # Case-insensitive
    ("Paula.Wyatt@FIRM.COM", "paula", "wyatt", "first.last"),
])
def test_classify_pattern(email, first, last, expected):
    assert classify_pattern(email, first, last) == expected


def test_classify_pattern_empty_inputs():
    assert classify_pattern("", "Paula", "Wyatt") is None
    assert classify_pattern("a@b.com", "", "") is None
    assert classify_pattern("a@b.com", "Paula", "") is None


# ──────────────────────────────────────────────────────────────────────
# NB verdict extraction
# ──────────────────────────────────────────────────────────────────────

def test_nb_verdict_from_dedicated_column():
    row = {"neverbounce_result": "valid", "email_source": ""}
    assert _nb_verdict_of(row) == "valid"


def test_nb_verdict_falls_back_to_email_source():
    """For rows scraped before neverbounce_result was being populated,
    the verdict lives in the email_source text."""
    row = {
        "neverbounce_result": None,
        "email_source": "industry prior '{first}.{last}' (law) — NeverBounce VALID",
    }
    assert _nb_verdict_of(row) == "valid"


def test_nb_verdict_catchall_normalized():
    row = {
        "neverbounce_result": None,
        "email_source": "industry prior '{f}{last}' — NeverBounce CATCH-ALL (unverified)",
    }
    assert _nb_verdict_of(row) == "catchall"


def test_nb_verdict_no_match():
    row = {"neverbounce_result": None, "email_source": "scraped from website"}
    assert _nb_verdict_of(row) == ""


# ──────────────────────────────────────────────────────────────────────
# Name extraction — strips titles / credentials
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("contact_name,expected_first,expected_last", [
    ("Paula Wyatt", "Paula", "Wyatt"),
    ("Dr. Alan Zabolian", "Alan", "Zabolian"),
    ("doctor Ori Levy", "Ori", "Levy"),
    ("Prof. Sarah Chen", "Sarah", "Chen"),
])
def test_first_last_strips_title(contact_name, expected_first, expected_last):
    row = {"contact_name": contact_name}
    first, last = _first_last_of(row)
    assert first == expected_first
    # Last may still have trailing credentials — not the focus of
    # THIS test. Just check the first name extraction.
    assert last.startswith(expected_last)


def test_first_last_strips_credentials():
    row = {"contact_name": "Lyan Zamora, DMD"}
    first, last = _first_last_of(row)
    assert first == "Lyan"
    assert last == "Zamora"


def test_first_last_empty_row():
    row = {"contact_name": ""}
    first, last = _first_last_of(row)
    assert first == ""
    assert last == ""


# ──────────────────────────────────────────────────────────────────────
# storage.update_business_emails — persists NB verdict
# ──────────────────────────────────────────────────────────────────────

def test_update_business_emails_persists_nb_from_dedicated_key():
    from unittest.mock import patch, MagicMock
    from src import storage

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    with patch.object(storage, "_connect", return_value=mock_conn), \
         patch.object(storage, "_cursor", return_value=mock_cur), \
         patch.object(storage, "init_db"):
        storage.update_business_emails(1, {
            "primary_email": "paula.wyatt@firm.com",
            "neverbounce_result": "valid",
            "email_safe_to_send": True,
        })
    sql, params = mock_cur.execute.call_args[0]
    assert "neverbounce_result" in sql.lower()
    # Find the NB param — second-to-last in the UPDATE (before id)
    assert "valid" in params


def test_update_business_emails_parses_nb_from_email_source_fallback():
    """When caller passes email_source with a NeverBounce suffix but no
    explicit neverbounce_result key, we parse it out."""
    from unittest.mock import patch, MagicMock
    from src import storage

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    with patch.object(storage, "_connect", return_value=mock_conn), \
         patch.object(storage, "_cursor", return_value=mock_cur), \
         patch.object(storage, "init_db"):
        storage.update_business_emails(1, {
            "primary_email": "paula.wyatt@firm.com",
            "email_source": "industry prior '{f}{last}' (law) — NeverBounce CATCH-ALL (unverified)",
        })
    _, params = mock_cur.execute.call_args[0]
    assert "catchall" in params


def test_update_business_emails_stores_none_when_no_verdict():
    from unittest.mock import patch, MagicMock
    from src import storage

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    with patch.object(storage, "_connect", return_value=mock_conn), \
         patch.object(storage, "_cursor", return_value=mock_cur), \
         patch.object(storage, "init_db"):
        storage.update_business_emails(1, {
            "primary_email": "x@y.com",
            "email_source": "scraped from website",
        })
    _, params = mock_cur.execute.call_args[0]
    assert None in params  # NB column set to NULL explicitly
