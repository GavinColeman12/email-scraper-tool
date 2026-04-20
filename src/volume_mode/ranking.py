"""
Volume-mode ranking bucket walker.

Priority (product rule — set by operator):

  a. Scraped personal email matching DM name         ← strongest
  b. DM email from triangulated pattern (≥2 evidence emails)
  c. Other scraped personal email (non-generic, non-DM)
  d. DM email from industry prior                    ← LAST RESORT
  e. first.last@ universal fallback                  ← only if no DM

Generic inboxes (info@, contact@, smile@, …) are NEVER picked.
They remain in evidence_trail.discovered_emails for pattern detection.

Within a bucket, NB-valid > NB-catchall > NB-unknown > not-tested.
First valid candidate in earliest bucket wins.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.volume_mode.stopwords import is_generic


# Confidence tier labels — map to the CSV Badge via shared export logic
TIER_VERIFIED = "volume_verified"   # 🟢  bucket a/b/c + NB-valid
TIER_SCRAPED = "volume_scraped"    # 🟡  bucket a/b/c + NB-catchall/unknown
TIER_GUESS = "volume_guess"        # 🔴  bucket d/e, not NB-verified
TIER_EMPTY = "volume_empty"        # ⚫  nothing usable


@dataclass
class Candidate:
    email: str
    bucket: str            # "a" | "b" | "c" | "d" | "e"
    pattern: str = ""      # e.g. "first.last", "scraped", "detected_pattern"
    source: str = ""       # descriptive label for the CSV
    nb_result: Optional[str] = None   # "valid" | "catchall" | "invalid" | "unknown" | None
    smtp_valid: bool = False

    def nb_rank(self) -> int:
        """Sort key — lower wins. NB-valid (0) > not-tested (1) > catchall (2) > unknown (3) > invalid (4)."""
        r = self.nb_result
        if r == "valid":
            return 0
        if r is None:
            return 1
        if r == "catchall":
            return 2
        if r == "unknown":
            return 3
        return 4  # invalid or anything else


BUCKET_ORDER = ("a", "b", "c", "d", "e")

# Buckets that point at the identified decision maker. When NB verifies
# these, we prefer them over a random scraped-personal (bucket c) — the
# goal is to reach the DM, not any person at the company.
DM_BUCKETS = ("a", "b", "d")


def pick_best(candidates: list[Candidate]) -> Optional[Candidate]:
    """
    Pick the best candidate with a two-tier priority:

    Tier 1 — NB-VALID candidates only:
        among DM-match buckets (a, b, d) → a > b > d
        else among scraped-other buckets (c, e) → c > e

    Tier 2 — nothing NB-valid, fall back to the original bucket walk:
        a → b → c → d → e
        Within bucket, NB-unknown > NB-catchall > NB-invalid > not-tested

    Generic-inbox emails are NEVER eligible.
    Bucket d/e candidates that came back NB-invalid are skipped (we have
    proof they bounce — don't send).
    """
    # Pre-filter: drop generics
    eligible = [c for c in candidates
                if not is_generic(c.email.split("@", 1)[0])]
    if not eligible:
        return None

    # Tier 1: any NB-valid? Prefer DM-match buckets, then bucket order.
    nb_valid = [c for c in eligible if c.nb_result == "valid"]
    if nb_valid:
        def _nb_valid_rank(c: Candidate) -> tuple:
            is_dm = 0 if c.bucket in DM_BUCKETS else 1
            return (is_dm, BUCKET_ORDER.index(c.bucket), c.email)
        nb_valid.sort(key=_nb_valid_rank)
        return nb_valid[0]

    # Tier 2: walk buckets a→e. Inside each bucket pick the best by NB rank.
    for bucket in BUCKET_ORDER:
        pool = [c for c in eligible if c.bucket == bucket]
        if not pool:
            continue
        pool.sort(key=lambda c: (c.nb_rank(), c.email))
        top = pool[0]
        # Bucket d/e NB-invalid = confirmed bounce, don't pick
        if bucket in ("d", "e") and top.nb_result == "invalid":
            continue
        return top
    return None


def confidence_tier(winner: Optional[Candidate]) -> str:
    """
    Map the winning candidate to a tier the shared Badge logic understands.

    volume_verified: any bucket (including d — industry-prior guess)
                     where NeverBounce returned VALID. When bucket D's
                     pattern-built email is NB-valid, we've PROVEN the
                     pattern — that's not a guess anymore.
    volume_scraped:  bucket a/b/c with NB-catchall/unknown/untested
    volume_guess:    bucket d/e NOT NB-verified (budget exhausted or
                     candidate never fit the NB walk)
    volume_empty:    nothing plausible
    """
    if winner is None:
        return TIER_EMPTY
    if winner.nb_result == "valid":
        return TIER_VERIFIED
    if winner.bucket in ("a", "b", "c"):
        return TIER_SCRAPED
    return TIER_GUESS
