"""Test suite for email_scoring.py."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import unittest
from src.email_scoring import (
    Specificity, classify_specificity,
    ScoringInputs, EmailScore, score_email_candidate,
    gate_decision, SendDecision,
    decay_score_by_age,
)


class TestSpecificityGenericInbox(unittest.TestCase):
    def test_info_is_generic_inbox(self):
        self.assertEqual(classify_specificity("info@clinic.com"), Specificity.GENERIC_INBOX)

    def test_hello_is_generic_inbox(self):
        self.assertEqual(classify_specificity("hello@clinic.com"), Specificity.GENERIC_INBOX)

    def test_contact_is_generic_inbox(self):
        self.assertEqual(classify_specificity("contact@clinic.com"), Specificity.GENERIC_INBOX)

    def test_admin_is_generic_inbox(self):
        self.assertEqual(classify_specificity("admin@clinic.com"), Specificity.GENERIC_INBOX)

    def test_reception_is_generic_inbox(self):
        self.assertEqual(classify_specificity("reception@clinic.com"), Specificity.GENERIC_INBOX)


class TestSpecificityGenericRole(unittest.TestCase):
    def test_billing(self):
        self.assertEqual(classify_specificity("billing@clinic.com"), Specificity.GENERIC_ROLE)

    def test_marketing(self):
        self.assertEqual(classify_specificity("marketing@clinic.com"), Specificity.GENERIC_ROLE)

    def test_appointments(self):
        self.assertEqual(classify_specificity("appointments@clinic.com"), Specificity.GENERIC_ROLE)

    def test_support(self):
        self.assertEqual(classify_specificity("support@clinic.com"), Specificity.GENERIC_ROLE)


class TestSpecificityPersonal(unittest.TestCase):
    def test_first_last(self):
        self.assertEqual(
            classify_specificity("tim.jones@clinic.com", "Tim", "Jones"),
            Specificity.PERSONAL,
        )

    def test_flast(self):
        self.assertEqual(
            classify_specificity("tjones@clinic.com", "Tim", "Jones"),
            Specificity.PERSONAL,
        )

    def test_dr_prefix(self):
        self.assertEqual(
            classify_specificity("dr.smith@clinic.com", "John", "Smith"),
            Specificity.PERSONAL,
        )

    def test_drsmith_no_dot(self):
        self.assertEqual(classify_specificity("drsmith@clinic.com"), Specificity.PERSONAL)

    def test_first_last_no_context(self):
        self.assertEqual(
            classify_specificity("bob.harrison@clinic.com"), Specificity.PERSONAL
        )

    def test_flast_no_context(self):
        self.assertEqual(classify_specificity("jsmith@clinic.com"), Specificity.PERSONAL)


class TestSpecificityTargetedRole(unittest.TestCase):
    def test_name_plus_office(self):
        self.assertEqual(
            classify_specificity("jessica.office@clinic.com"), Specificity.TARGETED_ROLE
        )

    def test_name_plus_manager(self):
        self.assertEqual(
            classify_specificity("tim.manager@clinic.com"), Specificity.TARGETED_ROLE
        )

    def test_name_plus_billing(self):
        self.assertEqual(
            classify_specificity("sarah.billing@clinic.com"), Specificity.TARGETED_ROLE
        )


class TestSpecificityEdgeCases(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(classify_specificity(""), Specificity.UNKNOWN)

    def test_malformed(self):
        self.assertEqual(classify_specificity("notanemail"), Specificity.UNKNOWN)

    def test_smile_is_not_personal(self):
        result = classify_specificity("smile@drsmile.com")
        self.assertIn(result, (Specificity.GENERIC_ROLE, Specificity.UNKNOWN))

    def test_owner_match_overrides(self):
        self.assertEqual(
            classify_specificity("tim@clinic.com", "Tim", "Jones"),
            Specificity.PERSONAL,
        )


class TestScoringTargetExamples(unittest.TestCase):
    def test_triangulated_nb_valid(self):
        inputs = ScoringInputs(
            email="jsmith@firm.com",
            owner_first="John", owner_last="Smith",
            owner_confidence=85, owner_title="Partner",
            was_generated_from_pattern=True,
            pattern_triangulated=True,
            pattern_confidence=88,
            pattern_evidence_count=2,
            nb_valid=True,
            is_catchall_domain=False,
        )
        result = score_email_candidate(inputs)
        self.assertEqual(result.specificity, Specificity.PERSONAL)
        self.assertEqual(result.grade, "A")
        self.assertGreaterEqual(result.score, 85)
        self.assertTrue(result.is_triangulated)

    def test_scraped_direct_nb_valid(self):
        inputs = ScoringInputs(
            email="drjones@mikejonesdds.com",
            owner_first="Michael", owner_last="Jones",
            owner_confidence=90, owner_title="Owner",
            was_scraped_direct=True, was_generated_from_pattern=False,
            pattern_triangulated=False,
            nb_valid=True,
            is_catchall_domain=False,
            owner_last_name_in_business=True,
        )
        result = score_email_candidate(inputs)
        self.assertEqual(result.specificity, Specificity.PERSONAL)
        self.assertEqual(result.grade, "A")
        self.assertGreaterEqual(result.score, 85)
        self.assertFalse(result.is_triangulated)

    def test_info_at_capped(self):
        inputs = ScoringInputs(
            email="info@clinic.com",
            owner_first="Tim", owner_last="Jones",
            owner_confidence=80, owner_title="Owner",
            was_scraped_direct=True, was_generated_from_pattern=False,
            nb_valid=True, is_catchall_domain=False,
        )
        result = score_email_candidate(inputs)
        self.assertEqual(result.specificity, Specificity.GENERIC_INBOX)
        self.assertIn(result.grade, ("C", "B"))
        self.assertLess(result.score, 75)
        self.assertLessEqual(result.components["source_evidence"], 15)

    def test_pure_guess_no_signals(self):
        inputs = ScoringInputs(
            email="jsmith@firm.com",
            owner_first="John", owner_last="Smith",
            owner_confidence=60, owner_title="",
            was_generated_from_pattern=True,
            pattern_triangulated=False,
            nb_unknown=True, is_catchall_domain=False,
        )
        result = score_email_candidate(inputs)
        self.assertEqual(result.grade, "D")
        self.assertLess(result.score, 50)


class TestScoringInvalidHardFail(unittest.TestCase):
    def test_nb_invalid_floors_to_zero(self):
        inputs = ScoringInputs(
            email="john.smith@firm.com",
            owner_first="John", owner_last="Smith",
            owner_confidence=95, owner_title="Owner",
            was_scraped_direct=True,
            pattern_triangulated=True, pattern_evidence_count=3,
            nb_invalid=True,
        )
        result = score_email_candidate(inputs)
        self.assertEqual(result.score, 0)
        self.assertEqual(result.grade, "F")


class TestScoringCatchallDegradation(unittest.TestCase):
    def test_nb_valid_on_catchall_is_weak(self):
        inputs = ScoringInputs(
            email="tim.jones@clinic.com",
            owner_first="Tim", owner_last="Jones",
            owner_confidence=80,
            was_generated_from_pattern=True,
            nb_valid=True, is_catchall_domain=True,
        )
        result = score_email_candidate(inputs)
        self.assertLessEqual(result.components["verification"], 10)

    def test_nb_valid_non_catchall_full(self):
        inputs = ScoringInputs(
            email="tim.jones@clinic.com",
            owner_first="Tim", owner_last="Jones",
            owner_confidence=80,
            was_generated_from_pattern=True,
            nb_valid=True, is_catchall_domain=False,
        )
        result = score_email_candidate(inputs)
        self.assertGreaterEqual(result.components["verification"], 25)


class TestScoringSynergyBonus(unittest.TestCase):
    def test_synergy_fires_on_triangulation_plus_nb(self):
        inputs = ScoringInputs(
            email="tim.jones@clinic.com",
            owner_first="Tim", owner_last="Jones",
            owner_confidence=80,
            was_generated_from_pattern=True,
            pattern_triangulated=True, pattern_confidence=88,
            pattern_evidence_count=2,
            nb_valid=True, is_catchall_domain=False,
        )
        result = score_email_candidate(inputs)
        self.assertGreaterEqual(result.components["synergy"], 10)

    def test_synergy_suppressed_on_catchall(self):
        inputs = ScoringInputs(
            email="tim.jones@clinic.com",
            owner_first="Tim", owner_last="Jones",
            owner_confidence=80,
            was_generated_from_pattern=True,
            pattern_triangulated=True, pattern_confidence=88,
            pattern_evidence_count=2,
            nb_valid=True, is_catchall_domain=True,
        )
        result = score_email_candidate(inputs)
        self.assertEqual(result.components.get("synergy", 0), 0)

    def test_synergy_requires_triangulation(self):
        inputs = ScoringInputs(
            email="tim.jones@clinic.com",
            owner_first="Tim", owner_last="Jones",
            was_generated_from_pattern=True,
            pattern_triangulated=False,
            nb_valid=True, is_catchall_domain=False,
        )
        result = score_email_candidate(inputs)
        self.assertEqual(result.components.get("synergy", 0), 0)


class TestScoringSpecificityCap(unittest.TestCase):
    def test_scraped_personal_full_30(self):
        inputs = ScoringInputs(
            email="drjones@clinic.com",
            was_scraped_direct=True, was_generated_from_pattern=False,
        )
        result = score_email_candidate(inputs)
        self.assertEqual(result.components["source_evidence"], 30)

    def test_scraped_generic_inbox_capped_at_15(self):
        inputs = ScoringInputs(
            email="info@clinic.com",
            was_scraped_direct=True, was_generated_from_pattern=False,
        )
        result = score_email_candidate(inputs)
        self.assertEqual(result.components["source_evidence"], 15)

    def test_scraped_generic_role_capped_at_20(self):
        inputs = ScoringInputs(
            email="billing@clinic.com",
            was_scraped_direct=True, was_generated_from_pattern=False,
        )
        result = score_email_candidate(inputs)
        self.assertEqual(result.components["source_evidence"], 20)


class TestGateDecisions(unittest.TestCase):
    def _score(self, **kw):
        defaults = dict(
            score=0, score_range_low=0, score_range_high=0,
            specificity=Specificity.PERSONAL, is_catchall=False,
            is_triangulated=False, requires_manual_review=False, grade="F",
        )
        defaults.update(kw)
        return EmailScore(**defaults)

    def test_invalid_score_skips(self):
        d = gate_decision(self._score(score=0, grade="F"))
        self.assertTrue(d.should_skip)
        self.assertFalse(d.should_send)

    def test_high_score_personal_sends(self):
        d = gate_decision(self._score(score=90, grade="A"))
        self.assertTrue(d.should_send)

    def test_generic_inbox_blocked(self):
        d = gate_decision(self._score(
            score=75, specificity=Specificity.GENERIC_INBOX, grade="B",
        ))
        self.assertFalse(d.should_send)
        self.assertTrue(d.should_skip)

    def test_catchall_no_triangulation_verify_further(self):
        d = gate_decision(self._score(
            score=65, is_catchall=True, is_triangulated=False, grade="C",
        ))
        self.assertTrue(d.should_verify_further)
        self.assertFalse(d.should_send)

    def test_catchall_with_triangulation_can_send(self):
        d = gate_decision(self._score(
            score=85, is_catchall=True, is_triangulated=True, grade="A",
        ))
        self.assertTrue(d.should_send)

    def test_manual_review_bucket(self):
        d = gate_decision(self._score(
            score=65, requires_manual_review=True, grade="C",
            warnings=["borderline"],
        ))
        self.assertTrue(d.should_manual_review)
        self.assertFalse(d.should_send)


class TestFreshnessDecay(unittest.TestCase):
    def test_zero_days(self):
        self.assertEqual(decay_score_by_age(85, 0), 85)

    def test_thirty_days(self):
        self.assertEqual(decay_score_by_age(85, 30), 83)

    def test_ninety_days(self):
        self.assertEqual(decay_score_by_age(85, 90), 79)

    def test_never_negative(self):
        self.assertEqual(decay_score_by_age(5, 365), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
