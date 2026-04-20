"""
Volume-mode pipeline — cheap mass email discovery.

Orchestrates:
  1. Deep website crawl (free, reuses universal_pipeline's 4-phase crawler)
  2. Wayback Machine snapshots (free, adds historical data)
  3. Name-candidate synthesis (free, reuses _synthesise_owners)
  4. LinkedIn fallback (paid ~$0.005, fires only when no DM found)
  5. Triangulation pattern detection (free)
  6. Build candidates into ranking buckets a/b/c/d/e
  7. Selective NeverBounce on buckets a/b/c only (paid ~$0.003/call, cap 3)
  8. Pick best_email via ranking walker — never generic inboxes
  9. Budget enforcement — degrade gracefully when cap hit

Output shape matches TriangulationResult so src/export_rows.py works
unchanged.
"""
from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field, asdict
from typing import Optional
from urllib.parse import urlparse

from src.volume_mode.priors import (
    build_email as _build_email,
    get_priors,
    normalize_vertical,
)
from src.volume_mode.ranking import (
    Candidate, pick_best, confidence_tier,
    TIER_VERIFIED, TIER_SCRAPED, TIER_GUESS, TIER_EMPTY,
)
from src.volume_mode.stopwords import is_generic, email_is_generic
from src.volume_mode.wayback import fetch_wayback_pages


logger = logging.getLogger(__name__)


# ── Budget (cost caps, per-biz and per-run) ──

BUDGET_PER_BIZ_USD = 0.025      # hard ceiling on API spend per biz
BUDGET_PER_RUN_DEFAULT_USD = 25.0
COST_LINKEDIN_CALL = 0.005
COST_NB_CALL = 0.003

# Run-level budget tracker — one global counter per process.
# For multi-run concurrency this would need to move to a per-job state.
_RUN_COST_USD = 0.0


def reset_run_budget(cap_usd: float = BUDGET_PER_RUN_DEFAULT_USD) -> None:
    global _RUN_COST_USD, _RUN_BUDGET_CAP
    _RUN_COST_USD = 0.0
    _RUN_BUDGET_CAP = cap_usd


_RUN_BUDGET_CAP = BUDGET_PER_RUN_DEFAULT_USD


def _run_budget_remaining() -> float:
    return max(0.0, _RUN_BUDGET_CAP - _RUN_COST_USD)


def _charge(amount_usd: float) -> None:
    global _RUN_COST_USD
    _RUN_COST_USD += amount_usd


# ── VolumeResult — adapter to src/export_rows.py schema ──

@dataclass
class VolumeResult:
    """Shaped to satisfy src/export_rows.build_row() via a storage dict."""
    best_email: str = ""
    best_email_confidence: int = 0
    best_email_evidence: list = field(default_factory=list)
    safe_to_send: bool = False
    risky_catchall: bool = False
    confidence_tier: str = TIER_EMPTY
    email_source: str = ""

    decision_maker: Optional["object"] = None  # universal_pipeline.OwnerCandidate
    all_owners: list = field(default_factory=list)
    detected_pattern: Optional["object"] = None
    candidate_emails: list = field(default_factory=list)  # list of dicts
    evidence_trail: dict = field(default_factory=dict)

    agents_run: list = field(default_factory=list)
    agents_succeeded: list = field(default_factory=list)
    time_seconds: float = 0.0
    cost_estimate: float = 0.0


# ── Main entry point ──

def scrape_volume(
    business: dict,
    *,
    use_neverbounce: bool = True,
    budget_per_biz_usd: float = BUDGET_PER_BIZ_USD,
    include_wayback: bool = True,
) -> VolumeResult:
    """
    Discover an email for a business using crawl + free signals first
    and paid APIs only as targeted fallbacks.

    Returns a VolumeResult shaped to feed the shared export row builder.
    """
    # Late imports to avoid circular dependency; universal_pipeline
    # imports from bounce_tracker which indirectly touches volume_mode
    # in some deployments.
    from src.universal_pipeline import (
        _Cache, get_cache,
        _agent_website_scrape,
        _agent_linkedin_gated,
        _synthesise_owners,
        _triangulate_pattern,
        _nb_verify_cached,
        _is_junk_name,
        OwnerCandidate,
    )

    t0 = time.time()
    result = VolumeResult()
    biz_cost = 0.0

    website = business.get("website") or ""
    if not website:
        result.time_seconds = round(time.time() - t0, 2)
        result.evidence_trail["exit_reason"] = "no_website"
        return result

    # Derive domain from website
    try:
        parsed = urlparse(website if website.startswith("http") else "https://" + website)
        domain = parsed.netloc.replace("www.", "").lower()
    except Exception:
        domain = ""
    if not domain:
        result.time_seconds = round(time.time() - t0, 2)
        result.evidence_trail["exit_reason"] = "no_domain"
        return result

    business_name = business.get("business_name") or ""
    cache = get_cache()

    # ── Phase 1: Deep website crawl (free) ──
    result.agents_run.append("website_scrape")
    all_candidates: list = []
    all_emails: set[str] = set()
    try:
        w_cands, w_emails = _agent_website_scrape(website, domain, business_name, cache)
        if w_cands or w_emails:
            result.agents_succeeded.append("website_scrape")
        all_candidates.extend(w_cands)
        all_emails.update(w_emails)
    except Exception as e:
        logger.warning(f"volume website_scrape: {e}")

    # ── Phase 2: Wayback (free, historical snapshots) ──
    if include_wayback and time.time() - t0 < 45:
        result.agents_run.append("wayback")
        try:
            pages = fetch_wayback_pages(domain, max_snapshots=5, deadline_s=15)
            if pages:
                result.agents_succeeded.append("wayback")
                # Parse the same way _agent_website_scrape does, but inline
                from src.universal_pipeline import (
                    _extract_names_with_titles, _strip_html,
                )
                from src.email_scraper import _is_rejected
                email_pat = re.compile(r"[A-Za-z0-9._%+-]+@" + re.escape(domain), re.I)
                for url, html in pages:
                    for e in email_pat.findall(html):
                        if not _is_rejected(e):
                            all_emails.add(e.lower())
                    try:
                        text = _strip_html(html)
                        for c in _extract_names_with_titles(text, business_name=business_name):
                            c.source = "wayback"
                            c.source_url = url
                            all_candidates.append(c)
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"wayback agent failed: {e}")

    # ── Phase 2.5: Haiku name classifier ──
    # Without this, service words like "Surgery Book" and "Teeth
    # Whitening" from page buttons get parsed as DM names and flood
    # bucket D with garbage. ~$0.001/biz (cached) is cheap insurance
    # against the stopword whack-a-mole and worth it in volume mode.
    if all_candidates:
        result.agents_run.append("llm_name_filter")
        try:
            from src.name_classifier import filter_real_people
            filtered = filter_real_people(
                all_candidates, business_name, domain, cache
            )
            if filtered is not None:
                before = len(all_candidates)
                all_candidates = filtered
                result.agents_succeeded.append("llm_name_filter")
                result.evidence_trail["llm_filter_removed"] = before - len(filtered)
        except Exception as e:
            logger.debug(f"volume llm name filter failed: {e}")

    # ── Phase 3: Synthesise DM from collected candidates ──
    ranked = _synthesise_owners(all_candidates, business_name)
    result.all_owners = ranked

    # Prefer the Google-Maps-supplied contact_name as a tiebreaker if it
    # survived the junk filter.
    hint_name = (business.get("contact_name") or "").strip()
    dm = None
    if hint_name:
        for c in ranked:
            if c.full_name.lower() == hint_name.lower():
                dm = c
                break
    if dm is None:
        dm = ranked[0] if ranked else None

    # ── Phase 4: LinkedIn fallback (paid, ONLY if no DM yet) ──
    if dm is None and use_neverbounce and _run_budget_remaining() > COST_LINKEDIN_CALL:
        if biz_cost + COST_LINKEDIN_CALL <= budget_per_biz_usd:
            result.agents_run.append("linkedin_fallback")
            try:
                li = _agent_linkedin_gated(business_name, False, cache)
                if li:
                    result.agents_succeeded.append("linkedin_fallback")
                    all_candidates.extend(li)
                    ranked = _synthesise_owners(all_candidates, business_name)
                    result.all_owners = ranked
                    dm = ranked[0] if ranked else None
                _charge(COST_LINKEDIN_CALL)
                biz_cost += COST_LINKEDIN_CALL
            except Exception as e:
                logger.debug(f"volume linkedin fallback: {e}")

    result.decision_maker = dm
    result.evidence_trail["discovered_emails"] = sorted(all_emails)

    # ── Phase 5: Triangulate pattern from evidence emails ──
    pattern = None
    if all_emails and ranked and domain:
        try:
            pattern = _triangulate_pattern(list(all_emails), ranked, domain, cache)
        except Exception:
            pattern = None
    result.detected_pattern = pattern

    # ── Phase 6: Build ranked candidate buckets ──
    candidates: list[Candidate] = []

    dm_first = (getattr(dm, "first_name", "") or "").lower() if dm else ""
    dm_last = (getattr(dm, "last_name", "") or "").lower() if dm else ""

    def _email_matches_dm(e: str) -> bool:
        if not (dm_first or dm_last):
            return False
        local = e.split("@", 1)[0].lower()
        if dm_first and dm_first in local:
            return True
        if dm_last and dm_last in local:
            return True
        # first-initial + last
        if dm_first and dm_last and (dm_first[0] + dm_last) in local:
            return True
        return False

    # Bucket A: scraped email matching DM name
    # Bucket C: scraped personal email (non-generic, non-DM-match)
    for email in sorted(all_emails):
        if not email.endswith("@" + domain):
            continue
        local = email.split("@", 1)[0]
        if is_generic(local):
            continue  # never a primary pick
        if _email_matches_dm(email):
            candidates.append(Candidate(
                email=email, bucket="a", pattern="scraped",
                source="scraped from website (matches decision maker)",
            ))
        else:
            candidates.append(Candidate(
                email=email, bucket="c", pattern="scraped",
                source="scraped from website (personal mailbox)",
            ))

    # Bucket B: DM email built from a TRIANGULATED pattern.
    # `{first}@` is still banned as an INDUSTRY-PRIOR guess (bucket D)
    # because bare-first is too ambiguous to try blind. But here in
    # bucket B, we have proof: the pattern was inferred from a real
    # email on this domain matching a real person at this business
    # (e.g. clark@ley.law → pattern first → george@ley.law for a
    # different partner). 1+ evidence email is enough when method is
    # triangulation.
    pattern_is_proven = (
        pattern and getattr(pattern, "method", "") == "triangulation"
        and bool(getattr(pattern, "evidence_emails", []) or [])
    )
    if dm and pattern and getattr(pattern, "confidence", 0) >= 70 and pattern_is_proven:
        # The existing triangulation pattern_name format differs from
        # volume_mode's {first}.{last} templating. Use universal_pipeline's
        # build_email for consistency with detected patterns (it handles
        # "first", "flast", "drlast", etc.).
        from src.industry_patterns import build_email as _up_build_email
        tri_email = _up_build_email(pattern.pattern_name, dm_first, dm_last, domain)
        if tri_email and not is_generic(tri_email.split("@", 1)[0]):
            # Don't add if already in a higher-bucket slot
            if not any(c.email == tri_email for c in candidates):
                candidates.append(Candidate(
                    email=tri_email, bucket="b",
                    pattern=pattern.pattern_name,
                    source=(
                        f"triangulated pattern '{pattern.pattern_name}' "
                        f"(evidence: {len(pattern.evidence_emails)} email"
                        f"{'s' if len(pattern.evidence_emails) != 1 else ''})"
                    ),
                ))

    # Bucket D: DM email from industry prior (LAST RESORT, primary only)
    # Guard: don't build a bucket-D email when the "DM" is actually the
    # business name being parsed as a person (e.g. "Franklin Barbecue"
    # → franklin.barbecue@franklinbbq.com). If the DM full name overlaps
    # heavily with the business name, it's almost certainly a false
    # extraction and we should fall back to empty rather than guess.
    # Role words that sometimes leak through the name extractor as a
    # "first name" — "Program Notion", "Service Experts", "Marketing
    # Director", etc. If the DM's first OR last name is in this set,
    # we refuse to build an industry-prior email for them.
    _ROLE_WORD_FIRSTS = {
        "program", "product", "project", "customer", "client", "account",
        "service", "services", "sales", "marketing", "support", "team",
        "staff", "admin", "office", "meeting", "event", "events", "contact",
        "brand", "design", "engineering", "operations", "ops", "creative",
        "content", "social", "community", "media", "press", "relations",
        "business", "finance", "human", "people", "talent",
        "general", "managing", "executive", "associate", "senior", "junior",
        "chief", "director", "manager", "partner", "founder", "president",
        "owner", "attorney", "lawyer", "doctor", "dentist", "practitioner",
        "principal",
    }

    def _dm_is_business_name_artifact() -> bool:
        if not dm:
            return True
        dm_tokens = set((dm.full_name or "").lower().split())
        biz_tokens = set((business_name or "").lower().split())
        # Drop common filler words that show up in business names
        for t in ("the", "and", "llc", "inc", "co", "corp", "group",
                  "of", "firm", "clinic", "practice", "center", "lab", "labs"):
            dm_tokens.discard(t); biz_tokens.discard(t)
        if not dm_tokens or not biz_tokens:
            return False
        overlap = dm_tokens & biz_tokens
        # If the DM name is ENTIRELY made of business-name tokens, it's junk
        if overlap and overlap == dm_tokens:
            return True
        # If the DM first OR last name is a known role-word, refuse to
        # build a guessed email — these are almost always extraction
        # noise ("Program Notion", "Service Experts", "Marketing
        # Director") not real people.
        if dm_first in _ROLE_WORD_FIRSTS or dm_last in _ROLE_WORD_FIRSTS:
            return True
        return False

    if dm and dm_first and dm_last and not _dm_is_business_name_artifact():
        industry_raw = (business.get("business_type") or "").lower()
        priors = get_priors(industry_raw)
        if priors:
            primary_pattern = priors[0]
            d_email = _build_email(primary_pattern, dm_first, dm_last, domain)
            if d_email and not is_generic(d_email.split("@", 1)[0]):
                # Only add if not already in higher bucket
                if not any(c.email == d_email for c in candidates):
                    vertical = normalize_vertical(industry_raw) or "generic"
                    candidates.append(Candidate(
                        email=d_email, bucket="d",
                        pattern=primary_pattern,
                        source=f"industry prior '{primary_pattern}' ({vertical})",
                    ))

    # Bucket E: universal first.last@ fallback (only if no DM was found
    # AND somehow we still have a name hint — rare)
    if not dm and hint_name:
        parts = hint_name.split()
        if len(parts) >= 2:
            e_email = _build_email("{first}.{last}", parts[0], parts[-1], domain)
            if e_email and not is_generic(e_email.split("@", 1)[0]):
                candidates.append(Candidate(
                    email=e_email, bucket="e", pattern="{first}.{last}",
                    source="fallback first.last@ (no DM found)",
                ))

    # ── Phase 7: NeverBounce verification ──
    # Walks candidates in priority order, NB'ing each until we find a
    # VALID one or exhaust the per-biz budget. The walk now includes
    # bucket D (DM industry-prior guess) — previously we skipped NB on
    # guesses, which meant a random scraped-person in bucket C beat an
    # actual DM whose pattern-built email would have NB-valid'd. Always
    # NB the DM's guess so we can promote it to volume_verified when it
    # works, or reject it when it bounces.
    #
    # Budget: up to 4 NB calls per biz ($0.012 worst case, well under the
    # $0.025/biz ceiling). Cached NB results cost $0 so most re-runs hit
    # 0 fresh calls.
    if use_neverbounce:
        result.agents_run.append("neverbounce")
        NB_BUDGET = 4
        # Priority for NB verification: DM-matching buckets first (a, b, d),
        # then scraped-other (c), then universal fallback (e). This is
        # DIFFERENT from the ranking walk — here we just want to ensure
        # the DM's candidate gets verified before we exhaust the budget
        # on random scraped emails.
        def _verify_priority(cand: Candidate) -> tuple:
            dm_match = 0 if cand.bucket in ("a", "b", "d") else 1
            bucket_idx = "abcde".index(cand.bucket)
            return (dm_match, bucket_idx)

        to_verify = sorted(candidates, key=_verify_priority)
        nb_used = 0
        found_valid = False
        for cand in to_verify:
            if nb_used >= NB_BUDGET:
                break
            if _run_budget_remaining() < COST_NB_CALL:
                break
            if biz_cost + COST_NB_CALL > budget_per_biz_usd:
                break
            # Skip if already NB'd (shouldn't happen, defence-in-depth)
            if cand.nb_result is not None:
                continue
            try:
                nb = _nb_verify_cached(cand.email, cache)
                cand.nb_result = nb.get("result")
            except Exception:
                cand.nb_result = None
            _charge(COST_NB_CALL)
            biz_cost += COST_NB_CALL
            nb_used += 1
            if cand.nb_result == "valid":
                found_valid = True
                # Found a deliverable address — stop burning budget on
                # lower-priority candidates.
                break
        if any(c.nb_result for c in candidates):
            result.agents_succeeded.append("neverbounce")

    # ── Phase 8: Pick best_email via ranking walker ──
    winner = pick_best(candidates)
    tier = confidence_tier(winner)
    result.confidence_tier = tier

    if winner is not None:
        result.best_email = winner.email
        result.best_email_evidence = [winner.source]
        if winner.nb_result == "valid":
            result.best_email_evidence.append("✅ NeverBounce: VALID")
            result.safe_to_send = True
            result.best_email_confidence = 95
        elif winner.nb_result == "catchall":
            result.best_email_evidence.append("⚠️ NeverBounce: CATCH-ALL")
            result.risky_catchall = True
            result.best_email_confidence = 70
        elif winner.nb_result == "unknown":
            result.best_email_evidence.append("ℹ️ NeverBounce: UNKNOWN")
            result.best_email_confidence = 55
        elif winner.nb_result == "invalid":
            result.best_email_evidence.append("❌ NeverBounce: INVALID")
            result.best_email_confidence = 0
        else:
            # Not NB-tested (bucket d/e or NB skipped)
            result.best_email_confidence = 40 if winner.bucket in ("d", "e") else 50

        # Safe to send: bucket a/b/c with NB-valid, or high-confidence scraped
        if tier == TIER_VERIFIED:
            result.safe_to_send = True
    else:
        result.best_email_confidence = 0

    result.email_source = winner.source if winner else "no_candidate_produced"

    # Serialise all candidates for the evidence trail (audit-tool consumers
    # read professional_ids.candidate_emails).
    result.candidate_emails = [
        {
            "email": c.email, "pattern": c.pattern, "source": c.bucket,
            "bucket": c.bucket, "nb_result": c.nb_result,
            "smtp_valid": c.smtp_valid, "smtp_catchall": False,
            "confidence": (95 if c.nb_result == "valid" else
                           70 if c.nb_result == "catchall" else
                           40 if c.bucket in ("d", "e") else 50),
        }
        for c in candidates
    ]

    result.time_seconds = round(time.time() - t0, 2)
    result.cost_estimate = round(biz_cost, 4)
    return result


# ── Adapter for storage.update_business_emails (same shape triangulation uses) ──

def volume_result_to_scrape_result(result: VolumeResult, business: dict) -> dict:
    """
    Convert a VolumeResult into the scrape_result dict that
    storage.update_business_emails persists. Same shape as the
    triangulation adapter in src/email_scraper.py so the downstream
    flow doesn't need to know which mode produced the row.
    """
    import json as _json

    # Descriptive email_source (new format)
    email_source = result.email_source or "volume_mode"
    # Append NB verdict suffix for parity with triangulation's labels
    top_nb = None
    for c in result.candidate_emails or []:
        if c.get("email") == result.best_email:
            top_nb = c.get("nb_result")
            break
    nb_suffix = ""
    if top_nb == "valid":
        nb_suffix = " — NeverBounce VALID"
    elif top_nb == "catchall":
        nb_suffix = " — NeverBounce CATCH-ALL (unverified)"
    elif top_nb == "invalid":
        nb_suffix = " — NeverBounce INVALID"
    elif top_nb == "unknown":
        nb_suffix = " — NeverBounce UNKNOWN"
    if result.confidence_tier in (TIER_GUESS, TIER_EMPTY) and not result.safe_to_send:
        nb_suffix += " [below threshold]"

    dm_name = result.decision_maker.full_name if result.decision_maker else ""
    dm_title = getattr(result.decision_maker, "title", "") if result.decision_maker else ""

    # confidence bucket for shared badge logic
    tier = result.confidence_tier
    if tier == TIER_VERIFIED:
        conf_bucket = "high"
    elif tier == TIER_SCRAPED:
        conf_bucket = "medium"
    elif tier == TIER_GUESS:
        conf_bucket = "low"
    else:
        conf_bucket = ""

    def _dm_dict():
        if not result.decision_maker:
            return None
        d = result.decision_maker
        return {
            "name": d.full_name,
            "npi": None,
            "credential": getattr(d, "title", ""),
            "source": getattr(d, "source", ""),
            "source_url": getattr(d, "source_url", ""),
        }

    professional_ids = {
        "decision_maker": _dm_dict(),
        "all_providers": [
            {
                "name": p.full_name,
                "npi": None,
                "credential": getattr(p, "title", "") or "",
                "source": getattr(p, "source", ""),
            }
            for p in (result.all_owners or [])
        ],
        "detected_pattern": ({
            "pattern": result.detected_pattern.pattern_name,
            "confidence": result.detected_pattern.confidence,
            "method": result.detected_pattern.method,
            "evidence_emails": result.detected_pattern.evidence_emails,
            "evidence_names": result.detected_pattern.evidence_names,
        } if result.detected_pattern else None),
        "agents_run": result.agents_run,
        "agents_succeeded": result.agents_succeeded,
        "time_seconds": result.time_seconds,
        "cost_estimate": result.cost_estimate,
        "candidate_emails": result.candidate_emails,
        "risky_catchall": result.risky_catchall,
        "confidence_tier": result.confidence_tier,
        "mode": "volume",
    }

    return {
        "primary_email": result.best_email or "",
        "scraped_emails": result.evidence_trail.get("discovered_emails", []) or [],
        "constructed_emails": [c.get("email") for c in result.candidate_emails if c.get("email")],
        "contact_name": dm_name,
        "contact_title": dm_title or "",
        "email_source": email_source + nb_suffix,
        "confidence": conf_bucket,
        "synthesis_reasoning": " | ".join(result.best_email_evidence[:3]),
        "synthesizer": "volume_v1",
        "professional_ids_json": _json.dumps(professional_ids, default=str),
        "triangulation_pattern": (result.detected_pattern.pattern_name
                                  if result.detected_pattern else None),
        "triangulation_confidence": result.best_email_confidence or None,
        "triangulation_method": ("volume_triangulation"
                                 if result.detected_pattern else "volume_industry_prior"),
        "email_safe_to_send": result.safe_to_send,
    }
