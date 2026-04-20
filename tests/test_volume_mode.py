"""Unit tests for volume-mode components — stopwords, priors, ranking."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.volume_mode.stopwords import is_generic, email_is_generic, GENERIC_LOCAL_PARTS
from src.volume_mode.priors import (
    INDUSTRY_PRIORS, get_priors, normalize_vertical, build_email, DEFAULT_PRIOR,
)
from src.volume_mode.ranking import (
    Candidate, pick_best, confidence_tier,
    TIER_VERIFIED, TIER_SCRAPED, TIER_GUESS, TIER_EMPTY,
)


# ── stopwords ──

def test_generic_flag_info_contact_smile():
    # The three the user explicitly called out
    assert is_generic("info")
    assert is_generic("contact")
    assert is_generic("smile")


def test_generic_flag_rejects_common_shared_inboxes():
    for g in ("hello", "hi", "team", "sales", "support", "admin", "noreply",
              "frontdesk", "appointments", "welcome", "webmaster"):
        assert is_generic(g), f"{g} should be generic"


def test_generic_flag_accepts_real_names():
    for name in ("marc", "jane", "john.smith", "drsmith", "sjones",
                 "abinash", "stacyfranklin"):
        assert not is_generic(name), f"{name} should NOT be generic"


def test_generic_short_and_numeric():
    # 2-char and numeric-only must be rejected regardless of list
    assert is_generic("ab")
    assert is_generic("12345")
    assert is_generic("")
    # Pure prefix matches too (info123, contact-us-today)
    assert is_generic("info123")
    assert is_generic("contact-us")  # direct hit
    assert is_generic("info-2")      # stripped match


def test_email_is_generic_passthrough():
    assert email_is_generic("info@acme.com")
    assert not email_is_generic("marc@acme.com")
    # malformed email = generic (safe default — don't pick)
    assert email_is_generic("notanemail")
    assert email_is_generic("")


# ── priors ──

def test_priors_have_no_bare_first_pattern():
    """{first}@ must never be in any industry's prior list."""
    for vertical, patterns in INDUSTRY_PRIORS.items():
        for p in patterns:
            assert p != "{first}", (
                f"vertical={vertical} has bare {{first}} prior — "
                "that's explicitly forbidden (too ambiguous to guess at)"
            )


def test_priors_every_vertical_has_at_least_two():
    for vertical, patterns in INDUSTRY_PRIORS.items():
        assert len(patterns) >= 2, f"{vertical} has fewer than 2 priors"


def test_normalize_vertical_aliases():
    assert normalize_vertical("Law firm") == "law"
    assert normalize_vertical("Dental clinic") == "dental"
    assert normalize_vertical("Med spa") == "medspa"
    assert normalize_vertical("Plumber") == "construction"
    assert normalize_vertical("Moving company") == ""  # not explicitly mapped, fine
    assert normalize_vertical("CPA firm") == "cpa"
    assert normalize_vertical("marketing agency") == "agency"


def test_get_priors_falls_back_to_default():
    assert get_priors("unknown weird industry") == DEFAULT_PRIOR
    assert get_priors("") == DEFAULT_PRIOR


def test_get_priors_law_is_firstname_lastname_first():
    priors = get_priors("Law firm")
    assert priors[0] == "{first}.{last}"
    assert priors[1] == "{f}{last}"


def test_get_priors_dental_keeps_drlast():
    priors = get_priors("Dental clinic")
    assert "dr{last}" in priors


def test_build_email_variants():
    # first.last
    assert build_email("{first}.{last}", "Jane", "Smith", "acme.com") == "jane.smith@acme.com"
    # flast
    assert build_email("{f}{last}", "Jane", "Smith", "acme.com") == "jsmith@acme.com"
    # drlast
    assert build_email("dr{last}", "Jane", "Smith", "acme.com") == "drsmith@acme.com"
    # last only
    assert build_email("{last}", "Jane", "Smith", "acme.com") == "smith@acme.com"
    # firstl (first + last-initial)
    assert build_email("{first}{l}", "Jane", "Smith", "acme.com") == "janes@acme.com"


def test_build_email_refuses_bare_first():
    # Defense in depth — if someone passes {first}, we refuse
    assert build_email("{first}", "Jane", "Smith", "acme.com") is None


def test_build_email_requires_both_names():
    assert build_email("{first}.{last}", "", "Smith", "acme.com") is None
    assert build_email("{first}.{last}", "Jane", "", "acme.com") is None


def test_build_email_unicode_names():
    # Accented names shouldn't break everything; we keep the letters
    r = build_email("{first}.{last}", "José", "García", "acme.com")
    assert r is not None and r.endswith("@acme.com")


# ── ranking ──

def test_pick_best_prefers_bucket_a_over_b():
    cands = [
        Candidate(email="jane@acme.com", bucket="b", nb_result="valid"),
        Candidate(email="jsmith@acme.com", bucket="a", nb_result="unknown"),
    ]
    winner = pick_best(cands)
    assert winner.email == "jsmith@acme.com"  # bucket a wins even vs b-NB-valid


def test_pick_best_skips_generic_even_if_top():
    # info@ in bucket A must be skipped entirely
    cands = [
        Candidate(email="info@acme.com", bucket="a", nb_result="valid"),
        Candidate(email="smith@acme.com", bucket="c", nb_result="unknown"),
    ]
    winner = pick_best(cands)
    assert winner.email == "smith@acme.com"


def test_pick_best_returns_none_when_all_generic():
    cands = [
        Candidate(email="info@acme.com", bucket="a", nb_result="valid"),
        Candidate(email="hello@acme.com", bucket="a", nb_result="valid"),
        Candidate(email="contact@acme.com", bucket="b", nb_result="valid"),
    ]
    assert pick_best(cands) is None


def test_pick_best_prefers_nb_valid_within_bucket():
    cands = [
        Candidate(email="one@acme.com", bucket="c", nb_result="catchall"),
        Candidate(email="two@acme.com", bucket="c", nb_result="valid"),
        Candidate(email="three@acme.com", bucket="c", nb_result=None),
    ]
    winner = pick_best(cands)
    assert winner.email == "two@acme.com"


def test_pick_best_skips_nb_invalid_in_guess_bucket():
    # If the only bucket-d candidate came back NB-invalid, return empty
    # (sending to a confirmed-bounce address is worse than sending nothing)
    cands = [
        Candidate(email="jane.smith@acme.com", bucket="d", nb_result="invalid"),
    ]
    assert pick_best(cands) is None


def test_confidence_tier_mapping():
    valid_a = Candidate(email="x@y", bucket="a", nb_result="valid")
    catchall_b = Candidate(email="x@y", bucket="b", nb_result="catchall")
    scraped_c = Candidate(email="x@y", bucket="c", nb_result=None)
    guess_d = Candidate(email="x@y", bucket="d", nb_result=None)
    assert confidence_tier(valid_a) == TIER_VERIFIED
    assert confidence_tier(catchall_b) == TIER_SCRAPED
    assert confidence_tier(scraped_c) == TIER_SCRAPED
    assert confidence_tier(guess_d) == TIER_GUESS
    assert confidence_tier(None) == TIER_EMPTY


if __name__ == "__main__":
    # Cheap self-runner so we don't require pytest installed
    import inspect
    funcs = [
        (name, obj) for name, obj in globals().items()
        if name.startswith("test_") and inspect.isfunction(obj)
    ]
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
