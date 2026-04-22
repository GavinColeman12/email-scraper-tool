"""
Replay explainer — turns raw replay snapshots into readable reasons.

The Replay page was a data dump. Operators needed an estimate of WHY
a business produced no email, and WHY a pick changed between before
and after. This module owns that interpretation layer.

Exports:
    explain_biz(replay_biz)      → BizExplanation  (single-biz read)
    explain_change(before, after) → ChangeReason    (A/B read)
    bucket_label(bucket_code)    → 'A · scraped DM-match'
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


BUCKET_LABELS = {
    "a": "A · scraped email matches DM",
    "b": "B · DM email from triangulated pattern",
    "c": "C · scraped personal (non-DM)",
    "d": "D · industry-prior guess",
    "e": "E · first.last@ fallback",
}


def bucket_label(code: str) -> str:
    return BUCKET_LABELS.get((code or "").lower(), f"? · {code}")


@dataclass
class BizExplanation:
    status: str              # 'found' | 'empty'
    severity: str            # 'ok' | 'warn' | 'fail'  (for emoji / color)
    reason: str              # 1-line human-readable
    winning_bucket: Optional[str] = None
    winning_pattern: Optional[str] = None
    nb_result: Optional[str] = None
    candidate_summary: str = ""
    dm_name: Optional[str] = None


# Older replays (triangulation-era) stored the candidate source as
# 'scraped_direct' / 'detected_pattern' / 'industry_prior' etc. instead
# of the volume-mode bucket letters. Map them so the explainer works
# on every historical replay.
_SOURCE_TO_BUCKET = {
    "scraped_direct": "c",       # may be A if it matches DM — resolved later
    "detected_pattern": "b",
    "industry_prior": "d",
    "first_last_fallback": "e",
}


def _bucket_of(cand: dict) -> str:
    """Return bucket letter for a candidate, old-format-compatible."""
    if cand.get("bucket"):
        return str(cand["bucket"]).lower()
    src = (cand.get("source") or "").lower()
    return _SOURCE_TO_BUCKET.get(src, "?")


def _candidate_summary(cands: list[dict]) -> str:
    """Compact 'bucket=valid/invalid' string for inline display."""
    if not cands:
        return "(no candidates built)"
    parts = []
    for c in cands:
        bucket = _bucket_of(c).upper()
        nb = c.get("nb_result") or "untested"
        parts.append(f"{bucket}:{nb}")
    return " · ".join(parts)


def explain_biz(biz_replay: dict) -> BizExplanation:
    """
    Reason-out why this biz produced (or failed to produce) an email.

    biz_replay is one row from replay_runs.businesses_json[*]['replay'].
    """
    best_email = (biz_replay or {}).get("best_email") or ""
    dm_obj = biz_replay.get("decision_maker") if biz_replay else None
    dm_name = (dm_obj or {}).get("full_name") if dm_obj else None
    cands = biz_replay.get("candidate_emails") or []

    # Find the winning candidate (matches best_email)
    winner = next(
        (c for c in cands if (c.get("email") or "").lower() == best_email.lower()),
        None,
    )

    summary = _candidate_summary(cands)

    if best_email and winner:
        bucket = _bucket_of(winner)
        nb = winner.get("nb_result")
        pattern = winner.get("pattern") or "?"
        # Compose reason from bucket + NB status
        if nb == "valid":
            if bucket == "a":
                reason = (f"🟢 Scraped directly from the website AND matches "
                          f"{dm_name or 'decision maker'}. NeverBounce confirmed valid.")
            elif bucket == "b":
                reason = (f"🟢 Triangulated pattern '{pattern}' from evidence emails "
                          f"on this domain, applied to {dm_name or 'DM'}. "
                          f"NeverBounce confirmed valid.")
            elif bucket == "c":
                reason = (f"🟢 Scraped from website. Real person, NB-valid — "
                          f"may not be the decision maker but is deliverable.")
            elif bucket == "d":
                reason = (f"🟢 Industry-prior guess '{pattern}' confirmed by NeverBounce. "
                          f"Guess worked — pattern for this domain is now proven.")
            else:
                reason = f"🟢 Pattern '{pattern}' in bucket {bucket.upper()} · NB valid."
            return BizExplanation("found", "ok", reason,
                                  winning_bucket=bucket, winning_pattern=pattern,
                                  nb_result=nb, candidate_summary=summary, dm_name=dm_name)

        if nb == "catchall":
            reason = (f"🟡 Picked '{pattern}' in bucket {bucket.upper()} but NB says "
                      f"catchall — domain accepts any address, so no mailbox guarantee.")
        elif nb == "unknown":
            reason = (f"🟣 Picked '{pattern}' in bucket {bucket.upper()} — NB returned "
                      f"UNKNOWN (out of credits, server refused, or edge case). "
                      f"Human review recommended before sending.")
        elif nb == "invalid":
            reason = (f"🔴 Picked '{pattern}' but NB says INVALID. Should not have "
                      f"been chosen — check ranking logic.")
        else:  # not NB-tested
            if bucket == "d":
                reason = (f"🔴 Industry-prior guess '{pattern}' applied to "
                          f"{dm_name or 'DM'}. Not NB-verified — treat as spray.")
            elif bucket == "e":
                reason = (f"🔴 Universal first.last@ fallback — no DM confirmed, "
                          f"guessing from name hint only.")
            else:
                reason = (f"🟡 Pattern '{pattern}' in bucket {bucket.upper()} · not NB-tested.")
        return BizExplanation("found",
                              "warn" if nb in ("catchall", "unknown") else "fail",
                              reason, winning_bucket=bucket, winning_pattern=pattern,
                              nb_result=nb, candidate_summary=summary, dm_name=dm_name)

    # No email — explain why not
    if not cands:
        if not dm_name:
            reason = ("❌ No decision maker identified. Deep crawl + LinkedIn "
                      "fallback couldn't surface a plausible person at this "
                      "business — probably a site with no staff page and a "
                      "name that Google search doesn't tie to an owner.")
            return BizExplanation("empty", "fail", reason,
                                  candidate_summary=summary, dm_name=None)
        reason = (f"❌ DM '{dm_name}' found but zero candidates were built. "
                  f"Likely the DM name looked like a business-name artifact "
                  f"(e.g. 'Franklin Barbecue' at Franklin Barbecue) or was a "
                  f"role-word extraction like 'Bonding Chao' — the guard "
                  f"refused to construct a guess.")
        return BizExplanation("empty", "warn", reason,
                              candidate_summary=summary, dm_name=dm_name)

    # Candidates exist but pick_best returned None → all generic or all NB-invalid
    invalid = [c for c in cands if c.get("nb_result") == "invalid"]
    catchall = [c for c in cands if c.get("nb_result") == "catchall"]
    untested = [c for c in cands if c.get("nb_result") in (None, "")]

    if invalid and len(invalid) == len(cands):
        patterns_tried = ", ".join(
            sorted({(c.get("pattern") or "?") for c in invalid})
        )
        reason = (f"📉 Every pattern tried came back INVALID — {dm_name or 'DM'} "
                  f"has no mailbox at '{patterns_tried}'. Domain strictly rejects "
                  f"unknown recipients. Genuinely unreachable by volume mode.")
        return BizExplanation("empty", "fail", reason,
                              candidate_summary=summary, dm_name=dm_name)

    if invalid and untested:
        reason = (f"📉 Primary pattern bounced ({len(invalid)} invalid); "
                  f"{len(untested)} untested patterns remain — NB budget may have "
                  f"been exhausted. Re-run with fresh NB credits to check secondary priors.")
        return BizExplanation("empty", "warn", reason,
                              candidate_summary=summary, dm_name=dm_name)

    if catchall and not untested:
        reason = (f"⚠️ All candidates returned CATCHALL — domain accepts any "
                  f"address so deliverability can't be confirmed. Volume mode "
                  f"correctly refused to pick one as safe.")
        return BizExplanation("empty", "warn", reason,
                              candidate_summary=summary, dm_name=dm_name)

    # All-generic case (filtered by pick_best)
    generic_like = [c for c in cands
                    if "@" in (c.get("email") or "")
                    and _looks_generic(c["email"].split("@", 1)[0])]
    if generic_like and len(generic_like) == len(cands):
        reason = ("⚫ Only generic/shared-inbox emails were findable (info@, "
                  "contact@, hello@, etc.). Volume mode never picks these — "
                  "this business has no deliverable DM email published.")
        return BizExplanation("empty", "warn", reason,
                              candidate_summary=summary, dm_name=dm_name)

    reason = (f"❓ Pipeline completed but no candidate qualified. DM={dm_name or 'none'}, "
              f"candidates={len(cands)}, invalid={len(invalid)}, untested={len(untested)}.")
    return BizExplanation("empty", "warn", reason,
                          candidate_summary=summary, dm_name=dm_name)


def _looks_generic(local: str) -> bool:
    """Cheap client-side generic check — keeps explain.py dependency-free."""
    lp = (local or "").lower()
    if not lp:
        return True
    if "info" in lp:
        return True
    for g in ("contact", "hello", "sales", "support", "admin", "office",
              "team", "mail", "help", "service", "reception", "noreply",
              "welcome", "smile", "alumni", "partners", "leadership"):
        if lp == g or lp.startswith(g):
            return True
    return False


@dataclass
class ChangeReason:
    change_type: str         # 'same' | 'email_changed' | 'newly_found' | 'newly_lost' | 'empty_both'
    severity: str            # 'gain' | 'loss' | 'neutral'
    reason: str              # human-readable 1-liner


def explain_change(before: dict, after: dict) -> ChangeReason:
    """
    Explain what changed between two replay snapshots of the same biz.
    before / after are dicts with keys `best_email`, `candidate_emails`,
    `decision_maker`, `confidence_tier` (the `replay` sub-dict per biz).
    """
    be = (before or {}).get("best_email") or ""
    ae = (after or {}).get("best_email") or ""
    bt = (before or {}).get("confidence_tier") or ""
    at = (after or {}).get("confidence_tier") or ""

    be_winner = _find_winner(before)
    ae_winner = _find_winner(after)

    if not be and not ae:
        return ChangeReason("empty_both", "neutral",
                            "No email in either run — persistently unreachable.")

    if be == ae:
        # Same email — did tier change?
        if bt != at:
            return ChangeReason(
                "tier_changed",
                "gain" if _tier_rank(at) > _tier_rank(bt) else "loss",
                f"Same email; tier moved {bt} → {at} (NB verdict changed).",
            )
        return ChangeReason("same", "neutral", "Same pick, same tier.")

    if be and not ae:
        # Had an email, now doesn't — regression
        why = "unknown"
        if be_winner and be_winner.get("nb_result") == "valid":
            why = "previously NB-valid pick now rejected — tighter generic filter or ranking fix"
        elif be_winner and be_winner.get("bucket") == "c":
            why = "scraped non-DM email (bucket C) demoted below the DM-guess which bounced"
        elif ae_winner and ae_winner.get("nb_result") == "invalid":
            why = "DM candidate now proven-invalid by NB; no alternate picked"
        return ChangeReason("newly_lost", "loss",
                            f"Had '{be}', now empty — {why}.")

    if not be and ae:
        # Empty before, has email now — recovery
        why = "unknown"
        if ae_winner:
            b = (ae_winner.get("bucket") or "?").lower()
            nb = ae_winner.get("nb_result") or "untested"
            p = ae_winner.get("pattern") or "?"
            if b == "d":
                why = f"new bucket-D guess '{p}' ({nb}) — multi-pattern fallback found one"
            elif b in ("a", "b"):
                why = f"bucket {b.upper()} lit up — DM now matched via '{p}'"
            else:
                why = f"new pick from bucket {b.upper()} pattern '{p}' ({nb})"
        return ChangeReason("newly_found", "gain",
                            f"Was empty, now '{ae}' — {why}.")

    # Both have emails, but different
    why = "unknown"
    b_bucket = _bucket_of(be_winner) if be_winner else "?"
    a_bucket = _bucket_of(ae_winner) if ae_winner else "?"
    b_nb = (be_winner or {}).get("nb_result") or "untested"
    a_nb = (ae_winner or {}).get("nb_result") or "untested"

    if b_bucket == "c" and a_bucket in ("a", "b", "d"):
        why = ("new ranking prioritized the DM's email over the random scraped person "
               f"(bucket {b_bucket.upper()} → {a_bucket.upper()})")
    elif a_bucket == "c" and b_bucket in ("a", "b", "d"):
        why = (f"DM candidate lost — fell back to scraped non-DM "
               f"(bucket {b_bucket.upper()} → {a_bucket.upper()})")
    elif a_nb == "valid" and b_nb != "valid":
        why = "secondary pattern confirmed by NB where primary had bounced"
    elif b_nb == "valid" and a_nb != "valid":
        why = "previously-valid pick now bounces (NB retest disagrees)"
    elif a_bucket == b_bucket:
        why = f"same bucket {a_bucket.upper()}, different pattern or email"

    delta = "gain" if _tier_rank(at) > _tier_rank(bt) else (
        "loss" if _tier_rank(at) < _tier_rank(bt) else "neutral")
    return ChangeReason("email_changed", delta,
                        f"'{be}' → '{ae}' — {why}.")


def _find_winner(snapshot: dict) -> Optional[dict]:
    best = (snapshot or {}).get("best_email") or ""
    if not best:
        return None
    for c in (snapshot.get("candidate_emails") or []):
        if (c.get("email") or "").lower() == best.lower():
            return c
    return None


def _tier_rank(tier: str) -> int:
    order = {"volume_verified": 4, "volume_review": 3, "volume_scraped": 2,
             "volume_guess": 1, "volume_empty": 0}
    return order.get(tier or "", 0)
