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


def pick_best(candidates: list[Candidate]) -> Optional[Candidate]:
    """
    Walk buckets a→e. Inside each bucket pick the best by NB rank.
    Skip any candidate whose local part is generic.
    Return None if no bucket had an acceptable candidate (caller
    should mark the row volume_empty).
    """
    for bucket in BUCKET_ORDER:
        pool = [c for c in candidates
                if c.bucket == bucket and not is_generic(c.email.split("@", 1)[0])]
        if not pool:
            continue
        pool.sort(key=lambda c: (c.nb_rank(), c.email))
        top = pool[0]
        # Within bucket (d) or (e), skip if NB actively invalid (we know it bounces)
        if bucket in ("d", "e") and top.nb_result == "invalid":
            continue
        return top
    return None


def confidence_tier(winner: Optional[Candidate]) -> str:
    if winner is None:
        return TIER_EMPTY
    if winner.bucket in ("a", "b", "c") and winner.nb_result == "valid":
        return TIER_VERIFIED
    if winner.bucket in ("a", "b", "c"):
        return TIER_SCRAPED
    return TIER_GUESS
