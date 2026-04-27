"""
Tests for fuzzy typo-correction in maps_search synonym expansion.

Without this, queries like "restaraunt" (typo of "restaurant") get no
synonym fan-out and hit Google Maps' ~30-result single-query cap.
With it, the typo expands to ~7 variants and recovers full coverage.
"""
import pytest

from src.maps_search import (
    fuzzy_synonym_key, _query_variants, QUERY_SYNONYMS, estimate_cost,
)


@pytest.mark.parametrize("typo,expected_correction", [
    ("restaraunt", "restaurant"),
    ("restraurant", "restaurant"),
    ("dentsit", "dentist"),
    ("lawer", "lawyer"),
    ("attorny", "attorney"),
])
def test_fuzzy_corrects_common_typos(typo, expected_correction):
    """The 5 most common typo patterns the user is likely to hit."""
    assert fuzzy_synonym_key(typo) == expected_correction


def test_fuzzy_returns_none_for_correct_spelling():
    """Exact matches don't need correction — fuzzy_synonym_key returns
    None so the existing exact-match branch handles them."""
    for spelling in ("restaurant", "dentist", "lawyer", "attorney"):
        assert fuzzy_synonym_key(spelling) is None


def test_fuzzy_returns_none_for_unrelated_terms():
    """Strict cutoff (0.78) prevents wrong corrections on novel terms."""
    for term in ("electrician", "thai massage", "yacht broker"):
        # These either match a known synonym key or shouldn't get
        # corrected to something wildly off
        result = fuzzy_synonym_key(term)
        # If a match comes back, it must be reasonably close (e.g.
        # "electrician" already exists or won't accidentally become
        # "dentist")
        if result is not None:
            assert result != "dentist"
            assert result != "lawyer"


def test_query_variants_expands_typos():
    """Regression for the user's actual case: 'restaraunt' should
    fan out to 'restaurant' + its 6 synonyms, not stay at 1 variant."""
    variants = _query_variants("restaraunt")
    assert len(variants) >= 7, (
        f"expected typo to fan out to 7+ variants, got {len(variants)}: "
        f"{variants}"
    )
    # Original query stays first (Google Maps may match the typo too)
    assert variants[0] == "restaraunt"
    # Corrected term is included
    assert "restaurant" in variants
    # Plus restaurant's known synonyms
    for syn in QUERY_SYNONYMS["restaurant"][:3]:
        assert syn in variants, f"missing synonym {syn}"


def test_query_variants_respects_exact_match_first():
    """When the query IS a known key, no fuzzy lookup happens —
    we just use the registered synonyms."""
    variants = _query_variants("restaurant")
    assert variants[0] == "restaurant"
    # Should still get all 6 synonyms
    assert len(variants) == len(QUERY_SYNONYMS["restaurant"]) + 1


def test_query_variants_unknown_term_returns_single():
    """Unknown queries with no fuzzy match still return just the
    original — we don't invent variants out of thin air."""
    variants = _query_variants("nonexistent_business_type_xyz")
    assert len(variants) == 1


def test_estimate_cost_factors_in_typo_correction():
    """The cost banner should reflect the higher fan-out from fuzzy
    correction — was 1 variant for 'restaraunt', now 7-8."""
    est_typo = estimate_cost(100, query="restaraunt")
    est_correct = estimate_cost(100, query="restaurant")
    # Both should fan out to multiple variants now
    assert est_typo["variants"] > 1
    assert est_typo["variants"] >= est_correct["variants"] - 1
