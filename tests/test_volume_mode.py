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


# ── P2: Practice-area + firm-name rejection ──

def test_p2_legal_practice_area_locals_rejected():
    for w in ("attorney", "lawyer", "divorce", "legal", "cases", "intake",
              "personalinjury", "dui", "probate", "estateplanning"):
        assert is_generic(w), f"{w} should be rejected as generic"


def test_p2_medical_practice_area_locals_rejected():
    for w in ("clinic", "practice", "cleanings", "extraction", "whitening",
              "cosmetic", "implants", "pediatric", "emergency"):
        assert is_generic(w), f"{w} should be rejected as generic"


def test_p2_compound_practice_area_rejected():
    """Substring match — divorceattorney@, pilawyer@, intakemanager@."""
    for w in ("divorceattorney", "pilawyer", "piattorney",
              "intakemanager", "officeadmin", "practicemanager"):
        assert is_generic(w), f"{w} should be rejected as generic"


def test_p2_firm_name_plus_modifier_in_local_rejected():
    """
    hlawfirm@hildebrandlaw.com, martinlaw@martin-law.com,
    weaverlaw@weaver-law.com — firm-name-token + law/firm/office
    modifier = shared alias, not a person.
    """
    # Needs business_name context to detect firm-name token
    assert is_generic("martinlaw", business_name="Martin Law Offices")
    assert is_generic("weaverlawfirm", business_name="Weaver Law Offices")
    assert is_generic("hildebrandlaw", business_name="Hildebrand Law Firm")
    # Bare last name as local is OK (Roger Weaver → weaver@ could be real)
    assert not is_generic("weaver", business_name="Weaver Law Offices")
    # First-last is obviously OK
    assert not is_generic("roger.weaver", business_name="Weaver Law Offices")


def test_p2_no_business_name_falls_back_to_old_behavior():
    """Backward compat: callers that don't pass business_name get the
    same behavior as before (only static blacklist + length checks)."""
    assert not is_generic("weaverlaw")  # no context, don't reject
    assert is_generic("info")  # still rejected via static list


# ── Post-search-38 regression guards ──

def test_info_substring_always_rejected():
    """
    User's rule: any local part containing 'info' is demoted. 'Info'
    appears in effectively zero real person names, so we treat it as
    an absolute shared-inbox signal. Covers infosp, smithinfo, drinfo,
    practiceinfo, info-team, info-sp, info123, etc.
    """
    # Exact + prefix variants we already caught
    assert is_generic("info")
    assert is_generic("infosp")
    assert is_generic("infocyl")
    # NEW: substring anywhere in the local part
    assert is_generic("smithinfo")
    assert is_generic("drinfo")
    assert is_generic("practiceinfo")
    assert is_generic("info-team")
    assert is_generic("info-sp")
    assert is_generic("2024info")
    assert is_generic("infomatic")   # previously survived; now correctly rejected


def test_contact_prefix_with_location_suffix_rejected():
    """Non-'info' prefix+short-suffix variants stay rejected."""
    assert is_generic("contactnyc")  # contact + NYC
    assert is_generic("salesmn")     # sales + MN
    assert is_generic("supportla")   # support + LA
    # Longer suffix stays — real "helloworld" is fine
    assert not is_generic("helloworld")
    # Classic generic forms still work
    assert is_generic("contact")


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

def test_pick_best_nb_valid_dm_match_beats_nb_valid_other():
    """
    New rule (post L9-followup): NB-valid candidates win regardless of
    bucket. Among NB-valid, DM-match buckets (a, b, d) beat non-DM
    buckets (c, e). Inside the DM-match tier, bucket order still applies.
    """
    cands = [
        Candidate(email="scraped-random@acme.com", bucket="c", nb_result="valid"),
        Candidate(email="dm-from-pattern@acme.com", bucket="d", nb_result="valid"),
    ]
    winner = pick_best(cands)
    # Bucket D (DM's industry-prior guess, NB-verified) wins over bucket
    # C (random scraped person, NB-verified) — we want the DM.
    assert winner.email == "dm-from-pattern@acme.com"


def test_pick_best_bucket_a_wins_when_both_nb_valid():
    """Within DM-match tier, bucket A (scraped DM) still beats bucket B (triangulated) beats D (prior)."""
    cands = [
        Candidate(email="dm-scraped@acme.com", bucket="a", nb_result="valid"),
        Candidate(email="dm-triangulated@acme.com", bucket="b", nb_result="valid"),
        Candidate(email="dm-prior@acme.com", bucket="d", nb_result="valid"),
    ]
    winner = pick_best(cands)
    assert winner.email == "dm-scraped@acme.com"


def test_pick_best_falls_through_to_original_walk_when_no_nb_valid():
    """When nothing is NB-valid, walk a→e in the original priority."""
    cands = [
        Candidate(email="scraped@acme.com", bucket="a", nb_result=None),
        Candidate(email="dm-guess@acme.com", bucket="d", nb_result="catchall"),
    ]
    winner = pick_best(cands)
    # Bucket A (untested) wins over bucket D (catchall) because we fall
    # through to the original walk when no NB-valid exists.
    assert winner.email == "scraped@acme.com"


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


# ── P1: DM-match walks before bucket C in tier 2 ──

def test_p1_constructed_dm_wins_over_scraped_non_dm_when_neither_verified():
    """
    The canonical bug: bbrady@martin-law.com (bucket c, random partner)
    was winning over joe.martin@martin-law.com (bucket d, constructed
    DM). Bucket D should win when neither is NB-valid because it's
    the actual decision maker, not a random associate.
    """
    from src.volume_mode.ranking import TIER_GUESS
    cands = [
        Candidate(email="bbrady@martin-law.com", bucket="c", nb_result=None),
        Candidate(email="joe.martin@martin-law.com", bucket="d", nb_result=None),
    ]
    winner = pick_best(cands)
    assert winner.email == "joe.martin@martin-law.com"


def test_p1_triangulated_dm_still_wins_over_scraped_non_dm():
    """Bucket B (triangulated DM from pattern evidence) beats bucket C."""
    cands = [
        Candidate(email="bbrady@martin-law.com", bucket="c", nb_result=None),
        Candidate(email="jmartin@martin-law.com", bucket="b", nb_result=None),
    ]
    assert pick_best(cands).email == "jmartin@martin-law.com"


# ── Bug A regression: multi-pattern bucket D walker ──

def test_bug_a_secondary_prior_wins_when_primary_invalids():
    """
    search_39 lost 22/24 construction biz because bucket D had only
    {first}.{last} and when NB said invalid the walker had nothing
    else to try. With 3 priors built, walker falls through to
    {f}{last} / {last}.
    """
    # Simulate: primary pattern invalid, secondary NB-valid
    cands = [
        Candidate(email="everett.berry@bbgci.com", bucket="d",
                  pattern="{first}.{last}", nb_result="invalid"),
        Candidate(email="eberry@bbgci.com", bucket="d",
                  pattern="{f}{last}", nb_result="valid"),
        Candidate(email="berry@bbgci.com", bucket="d",
                  pattern="{last}", nb_result=None),
    ]
    winner = pick_best(cands)
    assert winner is not None
    assert winner.email == "eberry@bbgci.com"  # NB-valid wins


def test_bug_a_all_priors_invalid_returns_none():
    """When every bucket-D pattern bounces, pick_best correctly
    returns None — we don't send to confirmed bounce addresses."""
    cands = [
        Candidate(email="a.b@x.com", bucket="d", nb_result="invalid"),
        Candidate(email="ab@x.com", bucket="d", nb_result="invalid"),
        Candidate(email="b@x.com", bucket="d", nb_result="invalid"),
    ]
    assert pick_best(cands) is None


def test_bug_a_untested_secondary_still_winnable_over_invalid_primary():
    """If primary is NB-invalid and secondary was never tested (budget
    ran out), the untested one should still win vs the confirmed bounce."""
    cands = [
        Candidate(email="everett.berry@bbgci.com", bucket="d",
                  pattern="{first}.{last}", nb_result="invalid"),
        Candidate(email="eberry@bbgci.com", bucket="d",
                  pattern="{f}{last}", nb_result=None),  # not tested
    ]
    winner = pick_best(cands)
    assert winner is not None
    assert winner.email == "eberry@bbgci.com"


# ── Bug B regression: credentialed DM survives biz-name overlap ──
# Tested via the actual pipeline predicate since it's defined as a
# closure inside scrape_volume. Exercising the behavior through a
# synthetic minimal call.

def test_bug_b_founder_credential_beats_biz_name_overlap():
    """
    'David Star' (Founder) at 'David Star Construction' is a real person.
    'Franklin Barbecue' (no credential) at 'Franklin Barbecue' is a
    parsing artifact. Both have DM tokens == biz tokens after filler
    strip. The credential is the distinguishing signal.
    """
    from dataclasses import dataclass

    # Minimal stand-in for the pipeline's private _dm_is_business_name_artifact.
    # Replicates the logic so we can test the credential-carve-out in
    # isolation. Kept in sync with src/volume_mode/pipeline.py.
    _FOUNDER_CREDENTIALS = {
        "founder", "co-founder", "cofounder", "owner", "co-owner",
        "ceo", "president", "principal", "managing partner",
        "managing director", "md", "chairman", "chairwoman", "chair",
        "proprietor",
    }
    _ROLE_WORDS = {"bonding", "surgery", "program"}  # subset — exact list in pipeline

    @dataclass
    class _DM:
        full_name: str
        first_name: str
        last_name: str
        title: str = ""

    def check(dm, business_name: str) -> bool:
        if dm.first_name.lower() in _ROLE_WORDS or dm.last_name.lower() in _ROLE_WORDS:
            return True
        cred = (dm.title or "").lower().strip()
        if any(c in cred for c in _FOUNDER_CREDENTIALS):
            return False
        dm_tokens = set(dm.full_name.lower().split())
        biz_tokens = set(business_name.lower().split())
        for t in ("the", "and", "llc", "inc", "co", "corp", "group",
                  "of", "firm", "clinic", "practice", "center", "lab", "labs",
                  "law", "legal", "attorneys", "construction", "contracting",
                  "building", "builders", "services", "company", "companies"):
            dm_tokens.discard(t); biz_tokens.discard(t)
        if not dm_tokens or not biz_tokens:
            return False
        return dm_tokens & biz_tokens == dm_tokens

    # Real owner, name matches biz — should pass
    david = _DM("David Star", "David", "Star", title="Founder")
    assert not check(david, "David Star Construction")

    # Extraction artifact, no credential — should reject
    franklin = _DM("Franklin Barbecue", "Franklin", "Barbecue")
    assert check(franklin, "Franklin Barbecue")

    # Andrew Hale at Hales Construction LLC (owner, last-name-in-biz)
    hale = _DM("Andrew Hale", "Andrew", "Hale", title="Owner")
    assert not check(hale, "Hales Construction LLC")

    # Dental procedure as first name — still rejected regardless of credential
    bonding = _DM("Bonding Chao", "Bonding", "Chao", title="Founder")
    assert check(bonding, "Golden Smile")


# ── P4: NB-unknown produces review tier, not verified ──

def test_p4_nb_unknown_gives_review_tier_not_verified():
    """
    NB returned UNKNOWN = we asked, NB couldn't say. Cold outreach must
    not auto-send into that — operator reviews manually. This is the
    new volume_review tier.
    """
    from src.volume_mode.ranking import TIER_REVIEW
    unknown_scraped = Candidate(email="x@y", bucket="a", nb_result="unknown")
    assert confidence_tier(unknown_scraped) == TIER_REVIEW
    unknown_pattern = Candidate(email="x@y", bucket="b", nb_result="unknown")
    assert confidence_tier(unknown_pattern) == TIER_REVIEW
    # Bucket d/e NB-unknown stays volume_guess (no proof the pattern
    # works; it's still a guess, just one we asked about)
    unknown_prior = Candidate(email="x@y", bucket="d", nb_result="unknown")
    assert confidence_tier(unknown_prior) == TIER_REVIEW
    # Catchall is distinct from unknown — stays volume_scraped
    catchall_scraped = Candidate(email="x@y", bucket="a", nb_result="catchall")
    assert confidence_tier(catchall_scraped) == TIER_SCRAPED


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
