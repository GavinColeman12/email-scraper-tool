"""
Rescue review-bucket rows — attempt to upgrade to send-safe before giving up.

When send_safety.classify_for_send() routes a row to "review", the row
is NOT necessarily unreachable. Two common reasons a row ends up there:

  1. NB returned "unknown" — could be rate limit, transient API error,
     or NB momentarily unavailable. The email may actually be valid.

  2. Name-mismatch — we picked a scraped shared inbox (hba@,
     patientbilling@, manager@) because every DM-pattern we tried
     came back NB-invalid. But we only tried 3 patterns; there are
     ~15 more worth trying before giving up on reaching the DM.

This module runs a "rescue" pass per review row:

  For NB-unknown rows:
    - Re-verify the primary_email with a fresh NB call

  For name-mismatch rows (shared inbox won, DM patterns all bounced):
    - Build ~15 additional DM pattern guesses we haven't tried
    - NB-verify the top candidates (budget ~$0.015/biz — 5 calls)
    - If any comes back valid, replace primary_email with it
    - Otherwise mark the row as "exhausted" so we don't retry again

Cost budget per row: ≤$0.018 (6 NB calls × $0.003). Caller can cap
the total budget across the batch.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional


logger = logging.getLogger(__name__)


COST_NB_CALL = 0.003
# Hard cap: 3 NB calls per row = $0.009. A batch of 100 review rows
# costs at most $0.90 — keeps rescue from ballooning when the gate
# holds back a lot of rows. If the row has NB=unknown, we spend 1
# call retrying + up to 2 on new patterns. If the row is a name-
# mismatch, we spend up to 3 on the 3 most-likely DM patterns.
DEFAULT_BUDGET_PER_ROW_USD = 0.009
DEFAULT_MAX_CANDIDATES = 3


# ── Pattern generator — broader than the volume_mode priors ──────────
# Ordered by "most likely to work" so we NB the good ones first and
# stop as soon as one comes back valid.
# Pattern name → (first, last, domain) → email generator.
# Keep in sync with learned_priors.classify_pattern so learned names
# can be translated back to email candidates.
_PATTERN_BUILDERS = {
    "first.last":   lambda f, l, d: f"{f}.{l}@{d}" if f and l else None,
    "flast":        lambda f, l, d: f"{f[0]}{l}@{d}" if f and l else None,
    "firstl":       lambda f, l, d: f"{f}{l[0]}@{d}" if f and l else None,
    "first_last":   lambda f, l, d: f"{f}_{l}@{d}" if f and l else None,
    "first-last":   lambda f, l, d: f"{f}-{l}@{d}" if f and l else None,
    "last.first":   lambda f, l, d: f"{l}.{f}@{d}" if f and l else None,
    "last_first":   lambda f, l, d: f"{l}_{f}@{d}" if f and l else None,
    "lastfirst":    lambda f, l, d: f"{l}{f}@{d}" if f and l else None,
    "first.l":      lambda f, l, d: f"{f}.{l[0]}@{d}" if f and l else None,
    "f.last":       lambda f, l, d: f"{f[0]}.{l}@{d}" if f and l else None,
    "fl":           lambda f, l, d: f"{f[0]}{l[0]}@{d}" if f and l else None,
    "first":        lambda f, l, d: f"{f}@{d}" if f and len(f) >= 3 else None,
    "last":         lambda f, l, d: f"{l}@{d}" if l and len(l) >= 4 else None,
    "drlast":       lambda f, l, d: f"dr{l}@{d}" if l else None,
    "dr.last":      lambda f, l, d: f"dr.{l}@{d}" if l else None,
    "drfirst":      lambda f, l, d: f"dr{f}@{d}" if f else None,
    "dr.first":     lambda f, l, d: f"dr.{f}@{d}" if f else None,
    "doctorlast":   lambda f, l, d: f"doctor{l}@{d}" if l else None,
}


def _extended_patterns(first: str, last: str, domain: str,
                        vertical: str = "",
                        max_candidates: int = DEFAULT_MAX_CANDIDATES,
                        learned_order: Optional[list[str]] = None,
                        ) -> list[tuple[str, str]]:
    """
    Return the TOP `max_candidates` pattern guesses for this DM.

    When `learned_order` is provided (from learned_priors), that's the
    primary ordering — slotted in first. Remaining slots are filled
    from a hardcoded fallback in case the learned list has fewer
    entries than max_candidates.

    Hardcoded fallback ordering (used when we don't have enough
    historical data for the vertical):
      Non-medical:  flast, first, first.last           (empirical)
      Dental/med:   drlast, flast, first.last          (empirical)

    These match what compute_learned_priors() returned on a real
    96-sample dataset — flast is the #1 universal pattern at ~44%,
    not first.last. The learner will override these as more campaigns
    feed the training set.
    """
    f = (first or "").lower().strip()
    l = (last or "").lower().strip()
    d = (domain or "").lower().strip()
    if not d:
        return []

    is_medical = any(
        v in (vertical or "").lower()
        for v in ("dental", "medical", "clinic", "dental clinic",
                    "dentist", "orthodontist", "oral", "chiropractic",
                    "medspa", "aesthetic", "veterinar")
    )

    # Fallback hardcoded priority — when no learned data exists
    if is_medical:
        hardcoded = ["drlast", "flast", "first.last", "dr.last",
                     "doctorlast", "first", "first_last", "last.first",
                     "drfirst", "dr.first", "first-last", "last",
                     "firstl", "f.last", "first.l", "fl"]
    else:
        hardcoded = ["flast", "first", "first.last", "first_last",
                     "last.first", "first-last", "last", "firstl",
                     "f.last", "first.l", "fl"]

    # Priority = learned_order first (if given), then hardcoded filler
    priority_names: list[str] = []
    seen_names: set = set()
    for name in (learned_order or []):
        if name not in seen_names:
            priority_names.append(name)
            seen_names.add(name)
    for name in hardcoded:
        if name not in seen_names:
            priority_names.append(name)
            seen_names.add(name)

    # Build the email candidates in priority order
    patterns: list[tuple[str, str]] = []
    seen_emails: set = set()
    for name in priority_names:
        builder = _PATTERN_BUILDERS.get(name)
        if not builder:
            continue
        try:
            candidate = builder(f, l, d)
        except Exception:
            candidate = None
        if not candidate or candidate in seen_emails:
            continue
        seen_emails.add(candidate)
        patterns.append((name, candidate))
        if len(patterns) >= max_candidates:
            break
    return patterns


# ── Rescue result ─────────────────────────────────────────────────────

@dataclass
class RescueResult:
    status: str                # "upgraded" | "exhausted" | "skipped"
    new_email: Optional[str] = None
    new_nb_result: Optional[str] = None
    reason: str = ""
    attempts: list[dict] = field(default_factory=list)
    cost_usd: float = 0.0


def rescue_review_row(
    biz: dict, *,
    nb_verify_fn=None,
    budget_usd: float = DEFAULT_BUDGET_PER_ROW_USD,
) -> RescueResult:
    """
    Attempt to upgrade a single review-bucket row.

    Args:
      biz: the business dict (from storage.list_businesses)
      nb_verify_fn: a callable(email) -> dict with 'result' key. Passed
        in so tests can stub it; falls back to src.neverbounce.verify.
      budget_usd: max NB spend on this row. Default $0.009 (3 calls).

    Returns a RescueResult with the new email (if upgraded) + attempts
    log. Does NOT write to the DB — caller is responsible for updating
    storage based on the result.
    """
    # Convert budget to integer call count to avoid floating-point
    # precision where 0.003 + 0.003 + 0.003 = 0.009000000000001 and
    # the final call gets wrongly skipped. Round up: $0.009 = 3 calls.
    max_calls = max(1, int(round(budget_usd / COST_NB_CALL)))
    # Lazy-import NB so the function is still testable without the SDK
    if nb_verify_fn is None:
        try:
            from src.neverbounce import verify as _nb_verify
            def nb_verify_fn(email: str):
                r = _nb_verify(email)
                return {
                    "result": getattr(r, "result", "unknown"),
                    "safe_to_send": getattr(r, "safe_to_send", False),
                }
        except Exception:
            return RescueResult(
                status="skipped",
                reason="NB module unavailable",
            )

    email = (biz.get("primary_email") or "").strip().lower()
    nb_result = (biz.get("neverbounce_result") or "").lower().strip()
    contact_name = (biz.get("contact_name") or "").strip()
    business_type = (biz.get("business_type") or "").strip()

    # Derive domain from email (or from website if email is empty)
    domain = ""
    if email and "@" in email:
        domain = email.split("@", 1)[1]
    else:
        website = biz.get("website") or ""
        if "://" in website:
            from urllib.parse import urlparse
            try:
                domain = (urlparse(website).hostname or "").replace("www.", "")
            except Exception:
                domain = ""

    if not domain:
        return RescueResult(status="skipped", reason="no domain")

    cost = 0.0
    calls_made = 0
    attempts: list[dict] = []

    # ── STRATEGY 1: NB unknown → re-verify the current email ────────
    # Transient NB errors (rate limit, timeout) often clear on retry.
    if nb_result == "unknown" and email:
        try:
            r = nb_verify_fn(email)
            cost += COST_NB_CALL
            calls_made += 1
            attempts.append({"email": email, "nb_result": r.get("result"),
                              "via": "retry_unknown"})
            new_result = (r.get("result") or "").lower()
            if new_result == "valid":
                return RescueResult(
                    status="upgraded",
                    new_email=email,
                    new_nb_result="valid",
                    reason="NB re-verify returned valid (was unknown)",
                    attempts=attempts, cost_usd=cost,
                )
            # If it's still unknown, fall through to strategy 2
            # (we might still find a better email via DM patterns).
        except Exception as e:
            attempts.append({"email": email, "error": str(e)})

    # ── STRATEGY 2: Build additional DM patterns, NB-verify top ones ──
    # For rows where the original scrape picked a shared inbox / wrong
    # person (name-mismatch), generate 15+ pattern guesses we didn't
    # try in the first pass and NB-verify until we find one valid.
    if contact_name and "@" not in contact_name:
        parts = contact_name.split(None, 1)
        dm_first = parts[0] if parts else ""
        dm_last = parts[1] if len(parts) > 1 else ""

        # Candidates we already tried live in candidate_emails (JSON
        # in the evidence trail). Skip those to avoid re-spending NB.
        already_tried: set = set()
        try:
            import json as _json
            prof = biz.get("professional_ids") or ""
            if isinstance(prof, str) and prof:
                pj = _json.loads(prof)
                for c in (pj.get("candidate_emails") or []):
                    e = (c.get("email") or "").lower().strip()
                    if e:
                        already_tried.add(e)
        except Exception:
            pass
        if email:
            already_tried.add(email)

        # Consult learned priors — if we have enough NB-valid history
        # for this vertical, use its top patterns instead of the
        # hardcoded fallback order.
        learned_order: list = []
        try:
            from src.learned_priors import top_patterns_for_vertical
            from src.dashboard_queries import normalize_vertical
            vertical_bucket = normalize_vertical(business_type)
            learned_order = top_patterns_for_vertical(
                vertical_bucket, top_n=5,
            )
        except Exception:
            learned_order = []

        new_patterns = _extended_patterns(
            dm_first, dm_last, domain, vertical=business_type,
            learned_order=learned_order,
        )
        # Skip ones we've already tried
        new_patterns = [(n, e) for n, e in new_patterns
                         if e.lower() not in already_tried]

        for pattern_name, candidate in new_patterns:
            if calls_made >= max_calls:
                # Hit the per-row cap — stop and mark exhausted
                break
            try:
                r = nb_verify_fn(candidate)
                cost += COST_NB_CALL
                calls_made += 1
                res = (r.get("result") or "").lower()
                attempts.append({"email": candidate, "pattern": pattern_name,
                                  "nb_result": res})
                if res == "valid":
                    return RescueResult(
                        status="upgraded",
                        new_email=candidate,
                        new_nb_result="valid",
                        reason=f"rescued via pattern {pattern_name!r}",
                        attempts=attempts, cost_usd=cost,
                    )
            except Exception as e:
                attempts.append({"email": candidate, "error": str(e)})
                continue

    # No upgrade possible — all remaining patterns invalid/unknown
    return RescueResult(
        status="exhausted",
        reason=(
            f"tried {len(attempts)} candidates, none NB-valid; "
            "original pick is the best we have"
        ),
        attempts=attempts, cost_usd=cost,
    )


def bulk_rescue(
    businesses: list[dict], *,
    total_budget_usd: float = 5.0,
    nb_verify_fn=None,
    per_row_budget_usd: float = DEFAULT_BUDGET_PER_ROW_USD,
    progress_cb=None,
) -> dict:
    """
    Rescue every review row in a list, respecting a total budget cap.
    Returns a summary dict:
      {
        "upgraded": [{"biz_id": N, "new_email": str, "old_email": str, ...}],
        "exhausted": [...],
        "skipped": [...],
        "total_cost_usd": float,
        "stopped_early": bool,  # hit the total budget
      }
    """
    out = {"upgraded": [], "exhausted": [], "skipped": [],
           "total_cost_usd": 0.0, "stopped_early": False}

    for i, biz in enumerate(businesses):
        if out["total_cost_usd"] + per_row_budget_usd > total_budget_usd:
            out["stopped_early"] = True
            break
        result = rescue_review_row(
            biz, nb_verify_fn=nb_verify_fn,
            budget_usd=min(per_row_budget_usd,
                            total_budget_usd - out["total_cost_usd"]),
        )
        out["total_cost_usd"] += result.cost_usd
        record = {
            "biz_id": biz.get("id"),
            "business_name": biz.get("business_name"),
            "old_email": biz.get("primary_email"),
            "new_email": result.new_email,
            "new_nb_result": result.new_nb_result,
            "reason": result.reason,
            "cost_usd": round(result.cost_usd, 4),
            "attempts": len(result.attempts),
        }
        out[result.status].append(record)
        if progress_cb:
            try:
                progress_cb(i + 1, len(businesses), result)
            except Exception:
                pass
    return out
