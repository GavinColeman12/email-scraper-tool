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
import re
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

from src.volume_mode.priors import (
    build_email as _build_email,
    get_priors,
    normalize_vertical,
)
from src.volume_mode.ranking import (
    Candidate, pick_best, confidence_tier,
    TIER_VERIFIED, TIER_SCRAPED, TIER_REVIEW, TIER_GUESS, TIER_EMPTY,
)
from src.volume_mode.stopwords import is_generic
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
    # Also reset the per-run domain pattern cache — cross-run bleed
    # would let yesterday's "kazi.law = {f}{last}" influence today's
    # unrelated biz on the same domain (edge case, cheap to prevent).
    try:
        from src.free_signals import clear_domain_cache
        clear_domain_cache()
    except Exception:
        pass


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
    rescue_empties_with_searchapi: bool = False,
) -> VolumeResult:
    """
    Discover an email for a business using crawl + free signals first
    and paid APIs only as targeted fallbacks.

    Args:
      rescue_empties_with_searchapi: when True, if volume's initial
        pass returns no DM (volume_empty), fire the combined
        owner+press SearchApi query as a last-ditch rescue. Costs
        ~$0.010 per rescued biz (only fires on empties) and can
        recover founders not on the team page / LinkedIn / Wayback.
        Default False because it's not free — opt-in per campaign.

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

    # Bail if the Google Maps record points at a known redirector / URL
    # shortener / listing aggregator (gtmaps.top, goo.gl, yelp.com, etc.).
    # Building patterns on these produces bogus emails like
    # sfaulkner@gtmaps.top that never existed. We flag the row and exit.
    try:
        from src.redirect_domains import is_redirect_domain
        if is_redirect_domain(domain):
            result.time_seconds = round(time.time() - t0, 2)
            result.evidence_trail["exit_reason"] = f"redirect_domain:{domain}"
            return result
    except Exception:
        pass

    business_name = business.get("business_name") or ""
    cache = get_cache()
    business_type = (business.get("business_type") or "").lower()
    address = (business.get("address") or business.get("location") or "")

    # ── Phase 0.5: NPI healthcare lookup (FREE, medical verticals only) ──
    # CMS.gov's National Provider Identifier registry is a free US
    # government API returning every licensed healthcare provider by
    # ZIP + taxonomy. For dental/medical/chiro/vet businesses this
    # gives us verified doctor names with credentials — often the
    # single highest-signal DM source. Zero cost, cached 30 days.
    is_medical_vertical = any(
        v in business_type
        for v in ("dental", "dentist", "orthodont", "endodont", "periodont",
                    "oral", "medical", "clinic", "doctor", "physician",
                    "chiro", "veterinar", "vet clinic", "animal hospital",
                    "optometrist", "pediatric", "dermat", "urgent care")
    )
    npi_candidates: list = []
    if is_medical_vertical and address:
        result.agents_run.append("npi_healthcare")
        try:
            from src.universal_pipeline import _agent_npi_healthcare
            npi_candidates = _agent_npi_healthcare(
                business_name, address, business_type, cache,
            )
            if npi_candidates:
                result.agents_succeeded.append("npi_healthcare")
        except Exception as e:
            logger.debug(f"volume NPI lookup failed: {e}")

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

    # Merge NPI candidates into the synthesis pool. High-signal source
    # for medical verticals — verified provider names with credentials.
    if npi_candidates:
        all_candidates.extend(npi_candidates)

    # ── Phase 1.3: Free-signal harvesters (WHOIS / LinkedIn slug /
    # footer / meta / CMS). Zero network beyond WHOIS (rdap.org), which
    # is free and cached 90d. Each returns dicts convertible to
    # OwnerCandidate so the synthesizer merges them uniformly.
    result.agents_run.append("free_signals")
    try:
        from src.free_signals import (
            whois_registrant_names, linkedin_slug_names,
            meta_author_names, footer_lastname_signals,
            get_domain_pattern,
        )
        from src.cms_detector import detect_cms, catchall_adjustment

        # WHOIS registrant — big signal on sole-proprietor domains.
        # Free; rdap.org; cached 90 days.
        try:
            wh_cands = whois_registrant_names(domain, cache)
            if wh_cands:
                all_candidates.extend([
                    OwnerCandidate(**c) for c in wh_cands
                ])
                result.agents_succeeded.append("whois_registrant")
        except Exception as e:
            logger.debug(f"whois_registrant: {e}")

        # Fetch the homepage ONCE for CMS/LinkedIn-slug/meta/footer.
        # Cached 14 days so re-runs are free. website_scrape's own
        # _fetch uses a separate path cache but the cost delta is
        # ~30ms per biz at worst.
        homepage_html = ""
        cache_key_hp = ("homepage_html", domain)
        cached_hp = cache.get(*cache_key_hp)
        if cached_hp is not None:
            homepage_html = cached_hp or ""
        else:
            try:
                import requests as _req
                _r = _req.get(
                    website if website.startswith("http") else f"https://{domain}/",
                    timeout=8,
                    headers={"User-Agent": "Mozilla/5.0 (volume-mode/1.0)"},
                    allow_redirects=True,
                )
                if _r.status_code == 200 and "text/html" in _r.headers.get("content-type", ""):
                    homepage_html = _r.text[:500_000]  # cap at 500KB
            except Exception:
                pass
            cache.set(cache_key_hp[0], homepage_html, cache_key_hp[1])

        if homepage_html:
            for c in linkedin_slug_names(homepage_html, domain):
                all_candidates.append(OwnerCandidate(**c))
            for c in meta_author_names(homepage_html):
                all_candidates.append(OwnerCandidate(**c))

            # Footer last-name signals → pattern evidence, not candidates
            footer_surnames = footer_lastname_signals(homepage_html)
            if footer_surnames:
                result.evidence_trail["footer_surnames"] = footer_surnames

            # CMS fingerprint — stashed for later NB interpretation
            cms_fp = detect_cms(homepage_html)
            if cms_fp:
                result.evidence_trail["cms"] = {
                    "cms": cms_fp.cms,
                    "confidence": cms_fp.confidence,
                    "catchall_hint": cms_fp.catchall_hint,
                    "provider_hint": cms_fp.provider_hint,
                    "evidence": cms_fp.evidence,
                }

        # Per-run domain pattern cache — if another biz at the same
        # domain already triangulated a pattern this run, inherit it.
        inherited = get_domain_pattern(domain)
        if inherited:
            result.evidence_trail["pattern_inherited_from_run"] = inherited
    except Exception as e:
        logger.debug(f"free_signals phase: {e}")

    # ── Phase 2: Wayback (free, historical snapshots) ──
    if include_wayback and time.time() - t0 < 45:
        result.agents_run.append("wayback")
        try:
            # Recent snapshots + one per historical year (corporate
            # redesigns scrub founder bios; 2018 often still has them).
            pages = fetch_wayback_pages(
                domain, max_snapshots=8, deadline_s=18,
                historical_years=[2020, 2017],
            )
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

    # ── Phase 2.9: Rescue-empty SearchApi fallback (opt-in, paid) ──
    # When website + wayback + LinkedIn + NPI all produced zero
    # candidates, the owner may simply not be on the live site. One
    # last-ditch SearchApi query ("[biz] owner / founder / CEO") mines
    # press mentions and third-party listings for the name. Costs
    # ~$0.010 per rescued biz; fires ONLY when rescue_empties flag is
    # on AND we have zero candidates after free signals. This replaces
    # what triangulation was doing on every biz — we now do it only
    # on the ~20% of rows where it's actually needed.
    if (rescue_empties_with_searchapi and not all_candidates
            and business_name and domain):
        result.agents_run.append("combined_owner_press_rescue")
        try:
            from src.universal_pipeline import _agent_combined_owner_and_press
            rescue_cands, rescue_emails, rescue_lis = (
                _agent_combined_owner_and_press(
                    business_name, domain, address, cache,
                )
            )
            if rescue_cands or rescue_emails:
                result.agents_succeeded.append("combined_owner_press_rescue")
                all_candidates.extend(rescue_cands)
                all_emails.update(rescue_emails)
                biz_cost += 0.010
                _charge(0.010)
        except Exception as e:
            logger.debug(f"combined_owner_press_rescue failed: {e}")

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

    # P3 override: for SMB domains like martin-law.com, wooleylawoffice.com,
    # dahlbergomeara.com — the last name embedded in the domain IS almost
    # always the founder. This beats LinkedIn ranking when it disagrees.
    # e.g. "Bill Brady" from LinkedIn vs "Joe Martin" from website — if
    # the domain is martin-law.com, Joe Martin wins.
    domain_core = (domain or "").lower()
    # Strip the TLD and split on non-alpha so "martin-law.com" →
    # segments {"martin", "law", "com"}; "dahlbergomeara.com" stays
    # as one blob "dahlbergomeara".
    import re as _re
    domain_no_tld = _re.sub(r"\.(com|org|net|co|law|us|io|biz|info).*$",
                             "", domain_core)
    domain_segments = set(_re.split(r"[^a-z]+", domain_no_tld))
    domain_segments.discard("")
    domain_segments.discard("law")
    domain_segments.discard("firm")
    domain_segments.discard("office")
    domain_segments.discard("offices")
    domain_segments.discard("group")
    domain_segments.discard("legal")
    domain_segments.discard("attorneys")
    domain_segments.discard("attorney")

    def _domain_contains_lastname(cand) -> bool:
        ln = (cand.last_name or "").lower()
        if len(ln) < 4:  # too short to reliably match
            return False
        # Exact segment match: domain_segments contain "martin" for Joe Martin
        if ln in domain_segments:
            return True
        # Substring in the continuous domain name (catches
        # "dahlbergomeara.com" with last_name="Dahlberg" or "OMeara")
        if ln in domain_no_tld:
            return True
        return False

    if ranked and domain_core:
        # If any provider's last name appears in the domain, that provider
        # is the founder — override whatever DM selection we had above.
        domain_founders = [c for c in ranked if _domain_contains_lastname(c)]
        if domain_founders:
            # Prefer the first one in synthesis order (already ranked by
            # signal strength). Skip this override if the current DM is
            # ALREADY a domain-founder (no need to change).
            current_is_founder = dm and _domain_contains_lastname(dm)
            if not current_is_founder:
                dm = domain_founders[0]
                result.evidence_trail["dm_override"] = (
                    f"domain_lastname: picked {dm.full_name} because "
                    f"'{dm.last_name}' appears in domain {domain_core}"
                )

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
        # Business / corporate roles
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
        # Dental / medical procedures leaking in as "first name"
        # (e.g. "Bonding Chao" on Golden Smile — "Bonding" is a
        # dental procedure label from the services page)
        "bonding", "crown", "crowns", "veneer", "veneers", "filling",
        "fillings", "extraction", "extractions", "cleaning", "cleanings",
        "whitening", "braces", "invisalign", "implant", "implants",
        "root", "canal", "periodontal", "cosmetic", "pediatric",
        "emergency", "exam", "exams", "checkup", "checkups",
        "xray", "x-ray", "consult", "consultation", "consultations",
        "treatment", "treatments", "procedure", "procedures",
        "surgery", "surgeries", "orthodontics", "orthodontic",
        # Legal practice areas as "first name"
        "divorce", "custody", "injury", "accident", "dui", "dwi",
        "bankruptcy", "probate", "immigration",
    }

    # Titles that strongly indicate the DM is a real founder/owner even
    # when their name overlaps with the business name. "David Star" is
    # a real person at "David Star Construction"; "Franklin Barbecue" is
    # not a real person at "Franklin Barbecue". The distinguishing
    # signal is whether the website-scrape extracted a credential like
    # "Founder" or "Owner" for them — a business name sitting on a
    # restaurant page has no such title attached.
    _FOUNDER_CREDENTIALS = {
        "founder", "co-founder", "cofounder", "owner", "co-owner",
        "ceo", "president", "principal", "managing partner",
        "managing director", "md", "chairman", "chairwoman", "chair",
        "proprietor",
    }

    def _dm_is_business_name_artifact() -> bool:
        if not dm:
            return True
        # If the DM first OR last name is a known role-word, refuse to
        # build a guessed email — these are almost always extraction
        # noise ("Program Notion", "Service Experts", "Marketing
        # Director", "Bonding Chao") not real people.
        if dm_first in _ROLE_WORD_FIRSTS or dm_last in _ROLE_WORD_FIRSTS:
            return True

        # If the DM has a real founder/owner credential, TRUST them even
        # when their name overlaps with the business name. Covers:
        #   "David Star" (Founder) at David Star Construction      ← real
        #   "Andrew Hale" (Owner)  at Hales Construction LLC       ← real
        #   "Franklin Barbecue"    at Franklin Barbecue (no cred)  ← artifact
        dm_cred = (getattr(dm, "title", "") or "").lower().strip()
        if any(c in dm_cred for c in _FOUNDER_CREDENTIALS):
            return False

        dm_tokens = set((dm.full_name or "").lower().split())
        biz_tokens = set((business_name or "").lower().split())
        # Drop common filler words that show up in business names
        for t in ("the", "and", "llc", "inc", "co", "corp", "group",
                  "of", "firm", "clinic", "practice", "center", "lab", "labs",
                  "law", "legal", "attorneys", "construction", "contracting",
                  "building", "builders", "services", "company", "companies"):
            dm_tokens.discard(t); biz_tokens.discard(t)
        if not dm_tokens or not biz_tokens:
            return False
        overlap = dm_tokens & biz_tokens
        # If the DM name is ENTIRELY made of business-name tokens AND
        # has no founder credential, it's likely a parsing artifact
        # (Franklin Barbecue, Pregnancy Discrimination, etc.)
        if overlap and overlap == dm_tokens:
            return True
        return False

    if dm and dm_first and dm_last and not _dm_is_business_name_artifact():
        industry_raw = (business.get("business_type") or "").lower()
        priors = get_priors(industry_raw)
        vertical = normalize_vertical(industry_raw) or "generic"
        # Build up to 3 bucket-D variants from the industry priors.
        # When the primary pattern NB-invalids, the walker falls through
        # to the secondary and tertiary. Without this, a single bounced
        # guess used to zero out the whole row — search_39 lost 22/24
        # construction businesses that way.
        for prior_pattern in priors[:3]:
            d_email = _build_email(prior_pattern, dm_first, dm_last, domain)
            if not d_email:
                continue
            if is_generic(d_email.split("@", 1)[0], business_name=business_name):
                continue
            if any(c.email == d_email for c in candidates):
                continue  # already in a higher bucket
            candidates.append(Candidate(
                email=d_email, bucket="d",
                pattern=prior_pattern,
                source=f"industry prior '{prior_pattern}' ({vertical})",
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
        # CMS-aware NB budget cut. Wix/Weebly/Duda/Shopify host mail
        # via their own platform which is effectively always catch-all
        # — NB will return "catchall" for almost any local part we
        # guess. Verifying bucket-D (industry prior) candidates on
        # these platforms burns budget on a foregone conclusion.
        # Skip the bucket-D guesses and only NB scraped addresses
        # (buckets a + c), which IS informative (real mailbox that
        # appeared on the live site).
        cms_info = result.evidence_trail.get("cms") or {}
        platform_mailbox_cms = cms_info.get("catchall_hint") == "real"

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
        nb_skipped_cms = 0
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
            # CMS skip: on platform-mailbox CMSes (Wix / Weebly / Duda
            # / Shopify / GoDaddy Builder), bucket-D/E guesses will
            # almost always NB as catchall. Save the call — mark as
            # 'catchall' via the CMS signal and move on. Scraped
            # emails (a / c) still get verified since they're
            # informative ground truth.
            if platform_mailbox_cms and cand.bucket in ("d", "e"):
                cand.nb_result = "catchall"
                nb_skipped_cms += 1
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
        if nb_skipped_cms:
            result.evidence_trail["nb_skipped_cms"] = nb_skipped_cms

    # ── Phase 8: Pick best_email via ranking walker + LLM final gate ──
    # The LLM gate takes the DM's name + the candidate list and picks
    # the most likely DM mailbox, rejecting generic inboxes and
    # wrong-person colleague emails that the rule-based walker used to
    # miss. One cached Haiku call per biz, ~$0.001.
    dm_name = result.decision_maker.full_name if result.decision_maker else ""
    dm_title = getattr(result.decision_maker, "title", "") if result.decision_maker else ""
    winner = pick_best(
        candidates, business_name=business_name,
        dm_name=dm_name, dm_title=dm_title,
        domain=domain, cache=cache,
    )
    # CMS-aware tier: Squarespace/Webflow catchalls get pushed to REVIEW
    # (they usually don't catchall, so the verdict is suspect); Wix /
    # Shopify / GoDaddy builder catchalls are trusted as real.
    cms_catchall_hint = "keep"
    try:
        from src.cms_detector import catchall_adjustment
        cms_info = result.evidence_trail.get("cms") or {}
        if cms_info:
            # Reconstruct a lightweight object for catchall_adjustment
            class _FP:
                pass
            fp = _FP()
            fp.cms = cms_info.get("cms")
            fp.catchall_hint = cms_info.get("catchall_hint", "unknown")
            nudge, rationale = catchall_adjustment(fp)
            cms_catchall_hint = nudge
            result.evidence_trail["cms_nudge"] = {"nudge": nudge, "rationale": rationale}
    except Exception:
        pass
    tier = confidence_tier(winner, cms_catchall_hint=cms_catchall_hint)
    result.confidence_tier = tier

    # Per-run domain pattern cache — stash triangulation result for
    # chains / multi-location biz that follow this one in the run.
    try:
        from src.free_signals import cache_domain_pattern
        if result.detected_pattern:
            cache_domain_pattern(domain, {
                "pattern_name": result.detected_pattern.pattern_name,
                "confidence": result.detected_pattern.confidence,
                "method": result.detected_pattern.method,
            })
    except Exception:
        pass

    # ── Post-pick DM correction ──
    # When the winning email's local part names a DIFFERENT provider than
    # the one we labeled as DM (Carlos Lorenzo vs jml@joselorenzolaw.com
    # where JML = Jose M Lorenzo), relabel the DM so the salutation
    # matches the actual recipient. Also promotes a domain-lastname
    # match over any incumbent DM — the person whose last name is in
    # the domain is almost always the founder.
    if winner and ranked:
        from src.name_equivalence import names_match
        winner_local = (winner.email.split("@", 1)[0].lower()
                        if "@" in winner.email else "")
        domain_core = domain.lower() if domain else ""
        # Strip TLD + common suffixes to isolate the "memorable" part
        import re as _re
        domain_tokens = set(_re.split(r"[^a-z]+", _re.sub(
            r"\.(com|org|net|co|law|us|io|biz|info).*$", "", domain_core)))
        domain_tokens -= {"", "law", "firm", "office", "group", "legal"}

        def _local_matches_provider(p) -> bool:
            first = (p.first_name or "").lower()
            last = (p.last_name or "").lower()
            if not (first and last):
                return False
            # Test every common local-part pattern against the provider
            patterns = {
                first, last,
                f"{first}.{last}", f"{first[0]}{last}",
                f"{first}{last}", f"{first[0]}.{last}",
                f"{first}{last[0]}",
            }
            # Nickname equivalence: jeff matches jeffrey, liz matches elizabeth
            extra_firsts = {ef for ef in
                            __import__("src.name_equivalence",
                                       fromlist=["equivalents"])
                            .equivalents(first)}
            for ef in extra_firsts:
                patterns |= {ef, f"{ef}.{last}", f"{ef[0]}{last}",
                             f"{ef}{last}"}
            return winner_local in patterns

        # Score candidates: strong match + domain-lastname wins
        best_provider = None
        best_score = -1
        for p in ranked:
            score = 0
            matches = _local_matches_provider(p)
            last = (p.last_name or "").lower()
            if matches:
                score += 5
            if last and len(last) >= 4 and last in domain_core:
                score += 3
            # Founder-title bonus
            title = (getattr(p, "title", "") or "").lower()
            if any(t in title for t in ("founder", "owner", "ceo",
                                         "president", "principal")):
                score += 1
            if score > best_score:
                best_score = score
                best_provider = p

        # Apply correction only when we found a strong alternate (≥5 =
        # had a name-pattern match). Otherwise keep the original DM.
        if best_provider and best_score >= 5 and dm and (
            best_provider.full_name.lower() != dm.full_name.lower()
        ):
            result.evidence_trail["dm_corrected_post_pick"] = (
                f"was '{dm.full_name}', corrected to '{best_provider.full_name}' "
                f"because winner local '{winner_local}' matches that provider's "
                f"name pattern{' and domain contains their last name' if best_score >= 8 else ''}"
            )
            dm = best_provider
            result.decision_maker = dm
        elif best_provider and best_score >= 8 and not dm:
            # No DM was chosen but we have a domain-lastname + name-match candidate
            dm = best_provider
            result.decision_maker = dm
            result.evidence_trail["dm_recovered_post_pick"] = (
                f"no DM before; assigned '{best_provider.full_name}' because "
                f"winner local '{winner_local}' matches this provider's name "
                f"AND last name is in the domain"
            )

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
    elif tier == TIER_REVIEW:
        # NB-unknown — human must review before send. Maps to a distinct
        # CSV badge via src/export_rows.verify_badge().
        conf_bucket = "review"
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
        # Dedicated NB column — feeds learned_priors.compute_learned_priors().
        # top_nb is the NB verdict on the winning email; normalized to
        # the same shape the storage UPDATE expects.
        "neverbounce_result": (top_nb or "").lower() or None,
        # CMS fingerprint — populated by the free_signals phase earlier
        # in this pipeline from detect_cms() on the homepage HTML.
        # Lifted from evidence_trail into dedicated columns so queries
        # can filter by CMS and learned_priors can cross-tab per
        # platform.
        "cms": (result.evidence_trail.get("cms") or {}).get("cms"),
        "cms_provider_hint": (result.evidence_trail.get("cms") or {}).get("provider_hint"),
        "cms_catchall_hint": (result.evidence_trail.get("cms") or {}).get("catchall_hint"),
    }
