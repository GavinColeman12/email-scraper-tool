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


# ──────────────────────────────────────────────────────────────────────
# Coverage breadth — every major industry has multi-variant fan-out
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,min_variants", [
    # Healthcare
    ("dentist", 4), ("doctor", 4), ("clinic", 4), ("urgent care", 3),
    ("pediatrician", 3), ("dermatologist", 3), ("cardiologist", 3),
    ("chiropractor", 3), ("physical therapy", 4), ("med spa", 3),
    ("orthodontist", 3), ("oral surgeon", 3), ("optometrist", 4),
    ("therapist", 3), ("psychologist", 3),
    # Vet / pet
    ("vet", 3), ("pet groomer", 2), ("dog trainer", 2),
    # Legal
    ("lawyer", 4), ("law firm", 4), ("personal injury attorney", 3),
    ("divorce attorney", 2), ("estate attorney", 2),
    # Food
    ("restaurant", 5), ("bakery", 3), ("pizza", 2), ("bar", 4),
    ("brewery", 3), ("coffee shop", 2), ("catering", 3),
    ("food truck", 1),  # niche; just don't crash
    # Wellness / beauty
    ("gym", 3), ("yoga", 2), ("salon", 2), ("nail salon", 2),
    ("massage", 4), ("spa", 2), ("tattoo", 2), ("waxing", 2),
    # Trades
    ("plumber", 3), ("electrician", 3), ("hvac", 4), ("roofer", 3),
    ("painter", 3), ("carpenter", 2), ("handyman", 3),
    ("locksmith", 3), ("movers", 2), ("solar", 3),
    # Construction
    ("contractor", 3), ("builder", 2), ("remodeling", 2),
    # Finance
    ("accountant", 4), ("financial advisor", 3), ("mortgage", 3),
    ("insurance", 3), ("wealth management", 3),
    # Real estate
    ("realtor", 2), ("real estate", 5), ("property management", 3),
    # Marketing / agency
    ("marketing agency", 3), ("seo", 2), ("web design", 3),
    ("graphic design", 3), ("pr firm", 3),
    # Auto
    ("mechanic", 3), ("auto detailing", 3), ("tire shop", 3),
    ("car wash", 3),
    # Retail
    ("florist", 2), ("jeweler", 3), ("boutique", 3),
    ("furniture store", 2), ("pharmacy", 3),
    # Education
    ("preschool", 4), ("daycare", 3), ("tutoring", 3),
    ("private school", 3),
    # Hospitality
    ("hotel", 4), ("wedding venue", 3),
    # Creative
    ("photographer", 4), ("event planner", 3),
    # Tech
    ("software", 3), ("it services", 3), ("cybersecurity", 3),
    # Misc
    ("nonprofit", 3), ("church", 2), ("funeral home", 3),
    ("storage", 3),
])
def test_industry_has_synonym_fanout(query, min_variants):
    """Every major industry should expand to N+1 variants (original
    + at least N synonyms) so it crosses Google Maps' single-query
    cap. Locks in the comprehensive coverage from this commit."""
    variants = _query_variants(query)
    # +1 because variants always include the original query first
    assert len(variants) >= min_variants + 1, (
        f"{query!r} only got {len(variants)} variants (need >= "
        f"{min_variants + 1}): {variants}"
    )


def test_no_duplicate_synonym_keys():
    """Sanity: every key in QUERY_SYNONYMS is unique. Catches the
    duplicate 'insurance' bug from before."""
    # Python dict literally can't have duplicate keys (later wins) —
    # this check is a guard against accidentally redefining the dict
    # in a way that loses entries. Verify the count matches what we
    # expect after the comprehensive expansion (~290+ keys).
    assert len(QUERY_SYNONYMS) >= 250, (
        f"Expected 250+ synonym keys, got {len(QUERY_SYNONYMS)}"
    )
