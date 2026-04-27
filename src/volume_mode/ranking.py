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
TIER_VERIFIED = "volume_verified"   # 🟢  NB-valid (deliverable confirmed)
TIER_SCRAPED = "volume_scraped"    # 🟡  scraped + NB-catchall (deliverable-looking but domain accepts everything)
TIER_REVIEW = "volume_review"      # 🟣  NB-unknown on a scraped/triangulated mailbox — cold outreach should NOT send without human review (NB couldn't verify, either quota or server-refused)
TIER_GUESS = "volume_guess"        # 🔴  industry-prior or fallback guess, not NB-verified — low confidence, send only as volume spray
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

# Priority walk when no NB-valid exists. DM-match buckets (a, b, d) come
# BEFORE non-DM scraped (c) — reversing the old a→b→c→d→e order.
# Rationale: a scraped non-DM email (bucket c, e.g. bbrady@martin-law.com
# when the DM is Joe Martin) should NOT beat a constructed DM email
# (bucket d, joe.martin@martin-law.com) even when neither is NB-verified.
# The scraped non-DM is valuable as pattern evidence, not as the
# primary send target.
DM_PRIORITY_WALK = ("a", "b", "d", "c", "e")


def pick_best(
    candidates: list[Candidate], *, business_name: str = "",
    dm_name: str = "", dm_title: str = "", domain: str = "",
    cache=None, use_llm: bool = True,
) -> Optional[Candidate]:
    """
    Pick the best candidate with a two-tier priority:

    Tier 1 — NB-VALID candidates only:
        among DM-match buckets (a, b, d) → a > b > d
        else among scraped-other buckets (c, e) → c > e

    Tier 2 — nothing NB-valid, walk DM_PRIORITY_WALK (a → b → d → c → e):
        DM-match buckets come before scraped-other.
        Within bucket, NB-unknown > NB-catchall > NB-invalid > not-tested.

    Generic-inbox emails are NEVER eligible. When `business_name` is
    passed, firm-name-as-local patterns (e.g. hlawfirm@hildebrandlaw.com,
    martinlaw@martin-law.com) are also rejected as generic.

    Bucket d/e NB-invalid = confirmed bounce, skip (don't send).
    """
    # Pre-filter: drop generics (including firm-name aliases when we have context)
    eligible = [c for c in candidates
                if not is_generic(
                    c.email.split("@", 1)[0],
                    business_name=business_name,
                )]
    if not eligible:
        return None

    # LLM final-gate — one Haiku call that takes the candidate list + DM
    # context and picks the best (or says NONE). Replaces the stopword
    # treadmill. Cached per (dm, biz, domain, candidates) so re-runs are
    # free. Falls through to rule-based walk when Haiku unavailable.
    #
    # Runs even when dm_name is empty — Haiku can still reject shared-
    # inbox candidates (connect@, reach@, catering@) based on the local
    # part alone, without a specific DM to match against. Previously the
    # empty-DM case silently fell through to rule-based walker, which is
    # how connect@jlc-law.com won on a biz where Phase 3 synthesis
    # returned no DM (filled in later by post-pick correction).
    if use_llm and cache is not None and eligible:
        try:
            from src.email_picker_llm import pick_email_with_llm
            llm_result = pick_email_with_llm(
                candidates=[{
                    "email": c.email, "bucket": c.bucket,
                    "pattern": c.pattern, "nb_result": c.nb_result,
                } for c in eligible],
                dm_name=dm_name, dm_title=dm_title,
                business_name=business_name, domain=domain, cache=cache,
            )
            if llm_result is not None:
                picked_email, _reason = llm_result
                if picked_email is None:
                    # Haiku says none of these reach the DM. Don't fall
                    # through — trust the LLM over the walker. Signal
                    # empty so the operator sees volume_empty, not a
                    # wrong-person pick. Rule-based walker is the one
                    # that historically picked bucket-C colleague emails.
                    return None
                # Match back to the Candidate object
                for c in eligible:
                    if c.email == picked_email:
                        return c
                # Pick doesn't match any candidate (shouldn't happen) —
                # fall through to rule-based walker as safety net
        except Exception:
            # LLM layer is strictly additive; failures fall through
            pass

    # Tier 1: any NB-valid? Prefer DM-match buckets, then bucket order.
    nb_valid = [c for c in eligible if c.nb_result == "valid"]
    if nb_valid:
        def _nb_valid_rank(c: Candidate) -> tuple:
            is_dm = 0 if c.bucket in DM_BUCKETS else 1
            return (is_dm, BUCKET_ORDER.index(c.bucket), c.email)
        nb_valid.sort(key=_nb_valid_rank)
        return nb_valid[0]

    # Tier 2: walk DM-match first (a → b → d) then scraped-other (c → e).
    for bucket in DM_PRIORITY_WALK:
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


def confidence_tier(winner: Optional[Candidate], *,
                    cms_catchall_hint: str = "keep") -> str:
    """
    Map the winning candidate to a tier the shared Badge logic understands.

    volume_verified: NB returned VALID. Safe to send.
    volume_review:   NB returned UNKNOWN. Cold outreach MUST NOT send —
                     NB couldn't verify (account out of credits, server
                     refused, or hit an edge case). Operator reviews
                     the row manually before the email goes out.
    volume_scraped:  bucket a/b/c with NB-catchall (deliverable-looking
                     but the mailbox may not exist on a catchall domain)
                     or NB-untested after budget exhaustion.
    volume_guess:    bucket d/e, not NB-verified — industry-prior guess
                     or universal fallback.
    volume_empty:    nothing plausible.
    """
    if winner is None:
        return TIER_EMPTY
    if winner.nb_result == "valid":
        return TIER_VERIFIED
    # SMTP-probe confirmed mailboxes — NB couldn't verify (often
    # greylisting on small-biz M365/Workspace domains) but our own
    # SMTP RCPT TO probe got a 250 OK. Real positive signal; tier as
    # scraped (volume_scraped) which IS sendable at low bounce risk.
    if winner.nb_result == "smtp_confirmed":
        return TIER_SCRAPED
    # NB-unknown specifically — we asked NB, it couldn't say. For cold
    # outreach at scale that's a bounce risk we shouldn't auto-send into.
    if winner.nb_result == "unknown":
        return TIER_REVIEW
    # NB-catchall + CMS-suspect (Squarespace with Google Workspace,
    # Webflow with custom mail): the catchall verdict is suspicious —
    # the specific mailbox might actually exist. Push to REVIEW so the
    # operator evaluates before sending instead of lumping into scraped.
    if winner.nb_result == "catchall" and cms_catchall_hint == "review":
        return TIER_REVIEW
    if winner.bucket in ("a", "b", "c"):
        return TIER_SCRAPED
    return TIER_GUESS
