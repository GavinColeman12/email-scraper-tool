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
DEFAULT_BUDGET_PER_ROW_USD = 0.018


# ── Pattern generator — broader than the volume_mode priors ──────────
# Ordered by "most likely to work" so we NB the good ones first and
# stop as soon as one comes back valid.
def _extended_patterns(first: str, last: str, domain: str,
                        vertical: str = "") -> list[tuple[str, str]]:
    """
    Return [(pattern_name, email), ...] for every additional pattern
    worth trying on this DM. Excludes patterns that are universally
    tried in the first pass (first.last, flast, firstl) since those
    already came back NB-invalid for review rows — but does include
    separator variants (first_last, first-last) that many chains use.

    `vertical` nudges which patterns are most likely first: dental/
    medical gets dr{last} / doctor{last} weighted up.
    """
    f = (first or "").lower().strip()
    l = (last or "").lower().strip()
    d = (domain or "").lower().strip()
    if not d:
        return []

    patterns: list[tuple[str, str]] = []

    # Separator variants — missed by the standard first.last build
    if f and l:
        patterns.append(("first_last", f"{f}_{l}@{d}"))
        patterns.append(("first-last", f"{f}-{l}@{d}"))

    # Reversed order — some firms use lastname first
    if f and l:
        patterns.append(("last.first", f"{l}.{f}@{d}"))
        patterns.append(("last_first", f"{l}_{f}@{d}"))
        patterns.append(("lastfirst", f"{l}{f}@{d}"))

    # First-name only (distinctive first names, startups)
    if f and len(f) >= 4:
        patterns.append(("first", f"{f}@{d}"))

    # Last-name only (partner firms, sole-prop)
    if l and len(l) >= 4:
        patterns.append(("last", f"{l}@{d}"))

    # Initial patterns we haven't tried
    if f and l:
        patterns.append(("f.last", f"{f[0]}.{l}@{d}"))      # j.smith
        patterns.append(("firstl", f"{f}{l[0]}@{d}"))       # johns
        patterns.append(("first.l", f"{f}.{l[0]}@{d}"))     # john.s
        patterns.append(("fl", f"{f[0]}{l[0]}@{d}"))        # js

    # Dental / medical — "dr" prefix is common for founder doctors
    is_medical = any(
        v in (vertical or "").lower()
        for v in ("dental", "medical", "clinic", "dental clinic",
                    "dentist", "orthodontist", "oral", "chiropractic",
                    "medspa", "aesthetic", "veterinar")
    )
    if is_medical and l:
        patterns.append(("dr.last", f"dr.{l}@{d}"))
        patterns.append(("doctor_last", f"doctor{l}@{d}"))
        if f:
            patterns.append(("drfirst", f"dr{f}@{d}"))
            patterns.append(("dr.first", f"dr.{f}@{d}"))

    # Dedup while preserving order — a pattern can map to the same
    # email as another (e.g. dr.last and doctorlast on single-syllable
    # lastnames); we only want to NB each unique address once
    seen = set()
    unique: list[tuple[str, str]] = []
    for name, email in patterns:
        if email in seen:
            continue
        seen.add(email)
        unique.append((name, email))
    return unique


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
      budget_usd: max NB spend on this row. Default $0.018 (6 calls).

    Returns a RescueResult with the new email (if upgraded) + attempts
    log. Does NOT write to the DB — caller is responsible for updating
    storage based on the result.
    """
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
    attempts: list[dict] = []

    # ── STRATEGY 1: NB unknown → re-verify the current email ────────
    # Transient NB errors (rate limit, timeout) often clear on retry.
    if nb_result == "unknown" and email:
        try:
            r = nb_verify_fn(email)
            cost += COST_NB_CALL
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

        new_patterns = _extended_patterns(
            dm_first, dm_last, domain, vertical=business_type,
        )
        # Skip ones we've already tried
        new_patterns = [(n, e) for n, e in new_patterns
                         if e.lower() not in already_tried]

        for pattern_name, candidate in new_patterns:
            if cost + COST_NB_CALL > budget_usd:
                # Hit the per-row cap — stop and mark exhausted
                break
            try:
                r = nb_verify_fn(candidate)
                cost += COST_NB_CALL
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
