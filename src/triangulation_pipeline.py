"""
Parallel-agent email triangulation pipeline (v3 — merged & cost-optimized).

5 parallel agents for email discovery:
1. NPI Providers — find ALL licensed providers at the practice
2. Website Email Discovery — deep scrape for any emails at the domain
3. Google Search for Provider Emails — "@domain" + business name
4. Press & News Search — owner quoted in news with email
5. SMTP Probe — parallel verification of top candidates

Strategy:
- If triangulation finds a pattern from 2+ providers: USE IT (confidence 88-95)
- If triangulation finds 1 match: USE IT (confidence 70)
- Otherwise: fall back to first.last@ (dominant B2B pattern at 10+ employee practices)
- SMTP-probe all candidates in parallel, NeverBounce-verify ONLY the top one
- Confidence threshold gate: safe_to_send only when score >= 70

Cost: ~$0.05-0.06 per business
  - NPI: free
  - SearchApi (Google + Press): 2 calls (~$0.03)
  - SMTP probes: free
  - NeverBounce: 1 verification ($0.003)

Time: 30-60 seconds per business (parallel agents).
"""
from __future__ import annotations

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests

from src.licensing_lookup import lookup_licensed_providers, parse_location
from src.email_sources import extract_all_hidden_emails
from src.industry_patterns import get_patterns_for, build_email
from src.neverbounce import verify as nb_verify
from src.email_verifier import verify_smtp


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class ProviderLookup:
    """A single provider at the practice (from NPI or other sources)."""
    full_name: str
    first_name: str
    last_name: str
    credential: Optional[str] = None
    npi: Optional[str] = None
    source: str = ""  # npi, website, press, google


@dataclass
class DetectedPattern:
    """A confirmed email pattern at the domain."""
    pattern_name: str
    confidence: int
    evidence_emails: List[str] = field(default_factory=list)
    evidence_names: List[str] = field(default_factory=list)
    method: str = "triangulation"


@dataclass
class TriangulationResult:
    """Final output of the triangulation pipeline."""
    decision_maker: Optional[ProviderLookup] = None
    all_providers: List[ProviderLookup] = field(default_factory=list)
    detected_pattern: Optional[DetectedPattern] = None

    best_email: Optional[str] = None
    best_email_confidence: int = 0
    best_email_evidence: List[str] = field(default_factory=list)
    safe_to_send: bool = False

    candidate_emails: List[dict] = field(default_factory=list)

    agents_run: List[str] = field(default_factory=list)
    agents_succeeded: List[str] = field(default_factory=list)
    time_seconds: float = 0.0
    cost_estimate: float = 0.0

    evidence_trail: Dict[str, object] = field(default_factory=dict)


# ============================================================
# MAIN PIPELINE
# ============================================================

def triangulate_email(
    business_name: str,
    website: str,
    domain: str,
    address: str,
    industry: str,
    decision_maker_hint: Optional[str] = None,
    scraped_emails: Optional[List[str]] = None,
    use_neverbounce: bool = True,
    confidence_threshold: int = 70,
) -> TriangulationResult:
    """Run discovery agents in parallel and synthesize the best email."""
    start = time.time()
    result = TriangulationResult()
    scraped_emails = scraped_emails or []

    # Parse address into NPI-friendly components once
    city, state, postal_code, street = parse_location(address) if address else ("", "", "", "")

    # ── Phase 1: Discovery (4 parallel agents) ───────────────────
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(
                _agent_npi_providers,
                business_name, industry,
                city, state, street, postal_code,
            ): "npi_providers",
            executor.submit(_agent_website_email_discovery, website, domain): "website_emails",
            executor.submit(_agent_google_search_for_providers, business_name, domain): "google_search",
            executor.submit(_agent_press_search, business_name, domain): "press_search",
        }

        agent_outputs: Dict[str, object] = {}
        for future in as_completed(futures):
            agent_name = futures[future]
            result.agents_run.append(agent_name)
            try:
                output = future.result(timeout=30)
                agent_outputs[agent_name] = output
                if (isinstance(output, list) and output) or (
                    isinstance(output, dict) and output.get("emails")
                ):
                    result.agents_succeeded.append(agent_name)
            except Exception as e:
                print(f"[triangulation] Agent {agent_name} failed: {e}")
                agent_outputs[agent_name] = None

    # SearchApi pricing is ~$0.005/credit; 2 searches ≈ $0.01, but add headroom
    result.cost_estimate += 0.03

    all_providers: List[ProviderLookup] = agent_outputs.get("npi_providers") or []
    website_data: dict = agent_outputs.get("website_emails") or {}
    google_data: dict = agent_outputs.get("google_search") or {}
    press_data: dict = agent_outputs.get("press_search") or {}

    result.all_providers = all_providers

    # Pick decision-maker
    if decision_maker_hint:
        result.decision_maker = _match_hint_to_provider(decision_maker_hint, all_providers)
    else:
        result.decision_maker = _pick_decision_maker(all_providers, business_name)

    if not result.decision_maker:
        result.time_seconds = time.time() - start
        result.evidence_trail["discovered_emails"] = list(set(
            scraped_emails
            + (website_data.get("emails") or [])
            + (google_data.get("emails") or [])
            + (press_data.get("emails") or [])
        ))
        return result

    # Aggregate every email we saw at this domain
    all_discovered_emails = list(set(
        scraped_emails
        + (website_data.get("emails") or [])
        + (google_data.get("emails") or [])
        + (press_data.get("emails") or [])
    ))

    result.evidence_trail["discovered_emails"] = all_discovered_emails
    result.evidence_trail["providers_count"] = len(all_providers)

    # ── Phase 2: Triangulate pattern from known names ────────────
    result.detected_pattern = _triangulate_pattern(
        emails=all_discovered_emails,
        providers=all_providers,
    )

    # ── Phase 3: Generate candidates ─────────────────────────────
    if not domain:
        result.time_seconds = time.time() - start
        return result

    candidates = _generate_candidates(
        decision_maker=result.decision_maker,
        domain=domain,
        detected_pattern=result.detected_pattern,
        industry=industry,
    )

    # ── Phase 4: SMTP probe in parallel (Agent 5) ────────────────
    if candidates:
        with ThreadPoolExecutor(max_workers=5) as executor:
            probe_futures = {executor.submit(_probe_smtp, c["email"]): c for c in candidates}
            for future in as_completed(probe_futures):
                candidate = probe_futures[future]
                try:
                    probe_result = future.result(timeout=15)
                    candidate["smtp_valid"] = probe_result.get("valid", False)
                    candidate["smtp_catchall"] = probe_result.get("catchall", False)
                except Exception:
                    candidate["smtp_valid"] = False
                    candidate["smtp_catchall"] = False

        result.agents_run.append("smtp_probe")
        if any(c.get("smtp_valid") for c in candidates):
            result.agents_succeeded.append("smtp_probe")

        # NeverBounce on the top candidate only — cost control
        top = max(candidates, key=lambda c: _candidate_confidence(c, result.detected_pattern))
        if use_neverbounce and top.get("smtp_valid"):
            try:
                nb = nb_verify(top["email"])
                top["nb_valid"] = nb.safe_to_send
                top["nb_result"] = nb.result
                result.cost_estimate += 0.003
            except Exception as e:
                top["nb_error"] = str(e)

        # Final ranking
        for c in candidates:
            c["confidence"] = _candidate_confidence(c, result.detected_pattern)
        candidates.sort(key=lambda c: c["confidence"], reverse=True)

        result.candidate_emails = candidates

        # ── Phase 5: Decision gate ────────────────────────────────
        top = candidates[0]
        result.best_email = top["email"]
        result.best_email_confidence = top["confidence"]
        result.best_email_evidence = _build_evidence(top, result)
        result.safe_to_send = top["confidence"] >= confidence_threshold

        if not result.safe_to_send:
            result.best_email_evidence.append(
                f"⚠️ Below threshold ({top['confidence']} < {confidence_threshold}) — review before sending"
            )

    result.time_seconds = time.time() - start
    return result


# ============================================================
# AGENT 1: NPI PROVIDERS
# ============================================================

def _agent_npi_providers(
    business_name: str,
    industry: str,
    city: str,
    state: str,
    street: str,
    postal_code: str,
) -> List[ProviderLookup]:
    """Get ALL licensed providers at the practice from NPI registry."""
    raw_providers = lookup_licensed_providers(
        vertical=industry,
        business_name=business_name,
        city=city,
        state=state,
        street_address=street,
        postal_code=postal_code,
    )

    providers: List[ProviderLookup] = []
    for p in raw_providers or []:
        # licensing_lookup uses keys: name, first, last, title, source
        first_raw = (p.get("first") or p.get("first_name") or "").strip()
        last_raw = (p.get("last") or p.get("last_name") or "").strip()
        if not first_raw or not last_raw:
            continue
        # Normalize: NPI returns names like "Se Jin" — pick the first token
        # for first-name and the last token for last-name when building
        # email patterns. Keep the full form for display.
        first_token = re.sub(r"[^A-Za-z]", "", first_raw.split()[0])
        last_token = re.sub(r"[^A-Za-z]", "", last_raw.split()[-1])
        if not first_token or not last_token:
            continue
        providers.append(ProviderLookup(
            full_name=f"{first_raw} {last_raw}",
            first_name=first_token,
            last_name=last_token,
            credential=p.get("title") or p.get("credential"),
            npi=p.get("npi"),
            source="npi",
        ))
    return providers


# ============================================================
# AGENT 2: WEBSITE EMAIL DISCOVERY
# ============================================================

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

_DEFAULT_PATHS = (
    "/", "/about", "/about-us", "/team", "/staff", "/providers",
    "/doctors", "/contact", "/contact-us", "/our-team", "/meet-the-team",
)


def _agent_website_email_discovery(website: str, domain: str) -> dict:
    """Deep-scrape the website for any email addresses at the domain."""
    if not website or not domain:
        return {"emails": []}

    base = website.rstrip("/")
    if not base.startswith("http"):
        base = "https://" + base

    emails = set()
    for path in _DEFAULT_PATHS:
        try:
            resp = requests.get(
                base + path,
                timeout=8,
                headers={"User-Agent": "Mozilla/5.0"},
                allow_redirects=True,
            )
            if resp.status_code != 200:
                continue
            # Hidden email extractors (Cloudflare / JSON-LD / obfuscated)
            hidden = extract_all_hidden_emails(resp.text)
            for source_emails in hidden.values():
                emails.update(source_emails)
            # Plain-text pass
            emails.update(_EMAIL_RE.findall(resp.text))
        except Exception:
            continue

    dom = domain.lower()
    domain_emails = sorted({e.lower() for e in emails if e.lower().endswith("@" + dom)})
    return {"emails": domain_emails, "all_emails_found": sorted(emails)}


# ============================================================
# AGENT 3: GOOGLE SEARCH FOR PROVIDER EMAILS
# ============================================================

def _agent_google_search_for_providers(business_name: str, domain: str) -> dict:
    """Google-search for emails at the target domain."""
    api_key = os.getenv("SEARCHAPI_KEY")
    if not api_key or not domain:
        return {"emails": []}

    query = f'"@{domain}" {business_name}'
    try:
        resp = requests.get(
            "https://www.searchapi.io/api/v1/search",
            params={"q": query, "engine": "google", "num": 10, "api_key": api_key},
            timeout=15,
        )
        data = resp.json()
    except Exception as e:
        return {"emails": [], "error": str(e)}

    pattern = re.compile(r"[A-Za-z0-9._%+-]+@" + re.escape(domain), re.IGNORECASE)
    emails = set()
    for row in (data.get("organic_results") or []):
        snippet = (row.get("snippet") or "") + " " + (row.get("title") or "")
        emails.update(m.lower() for m in pattern.findall(snippet))
    return {"emails": sorted(emails)}


# ============================================================
# AGENT 4: PRESS & NEWS SEARCH
# ============================================================

def _agent_press_search(business_name: str, domain: str) -> dict:
    """Search press/news for owner mentions that include an email."""
    api_key = os.getenv("SEARCHAPI_KEY")
    if not api_key or not domain:
        return {"emails": []}

    query = f'"{business_name}" (owner OR founder OR CEO OR partner) email'
    try:
        resp = requests.get(
            "https://www.searchapi.io/api/v1/search",
            params={"q": query, "engine": "google", "num": 5, "api_key": api_key},
            timeout=15,
        )
        data = resp.json()
    except Exception as e:
        return {"emails": [], "error": str(e)}

    pattern = re.compile(r"[A-Za-z0-9._%+-]+@" + re.escape(domain), re.IGNORECASE)
    emails = set()
    for row in (data.get("organic_results") or []):
        snippet = (row.get("snippet") or "") + " " + (row.get("title") or "")
        emails.update(m.lower() for m in pattern.findall(snippet))
    return {"emails": sorted(emails)}


# ============================================================
# TRIANGULATION LOGIC
# ============================================================

_GENERIC_LOCALS = {
    "info", "hello", "contact", "admin", "office", "team", "help",
    "support", "sales", "billing", "reception", "appointments",
    "frontdesk", "front.desk", "care", "noreply", "no-reply",
}


def _triangulate_pattern(
    emails: List[str],
    providers: List[ProviderLookup],
) -> Optional[DetectedPattern]:
    """Match discovered emails against known providers to infer the practice's pattern."""
    if not emails or not providers:
        return None

    pattern_votes: Dict[str, List[tuple]] = {}

    for email in emails:
        if "@" not in email:
            continue
        local = email.split("@")[0].lower()
        if local in _GENERIC_LOCALS:
            continue

        for provider in providers:
            first = provider.first_name.lower()
            last = provider.last_name.lower()
            detected = _classify_pattern_for_name(local, first, last)
            if detected:
                pattern_votes.setdefault(detected, []).append((email, provider.full_name))
                break

    if not pattern_votes:
        return None

    pattern_name, evidence = max(pattern_votes.items(), key=lambda kv: len(kv[1]))
    vote_count = len(evidence)
    if vote_count >= 3:
        confidence = 95
    elif vote_count == 2:
        confidence = 88
    else:
        confidence = 70

    return DetectedPattern(
        pattern_name=pattern_name,
        confidence=confidence,
        evidence_emails=[e for e, _ in evidence],
        evidence_names=[n for _, n in evidence],
        method="triangulation",
    )


def _classify_pattern_for_name(local: str, first: str, last: str) -> Optional[str]:
    """Given an email local part and a known name, decide which pattern was used."""
    if not first or not last:
        return None
    f0 = first[0]
    if local == f"{first}.{last}":
        return "first.last"
    if local == f"{first}{last}":
        return "firstlast"
    if local == f"{f0}{last}":
        return "flast"
    if local == f"{f0}.{last}":
        return "f.last"
    if local == first and len(first) >= 4:
        return "first"
    if local == last:
        return "last"
    if local == f"{last}.{first}":
        return "last.first"
    if local == f"{last}{f0}":
        return "lastf"
    if local == f"dr.{last}":
        return "dr.last"
    if local == f"dr{last}":
        return "drlast"
    if local == f"dr.{first}":
        return "dr.first"
    return None


# ============================================================
# CANDIDATE GENERATION
# ============================================================

def _generate_candidates(
    decision_maker: ProviderLookup,
    domain: str,
    detected_pattern: Optional[DetectedPattern],
    industry: str,
) -> List[dict]:
    """
    Return up to 4 candidate emails:
      1. Detected pattern (if confidence >= 70)
      2. first.last@ — dominant fallback for 10+ employee practices
      3. Top industry priors (excluding the 'first' pattern since we target 10+ HC)
    """
    first = decision_maker.first_name
    last = decision_maker.last_name
    candidates: List[dict] = []
    seen = set()

    # Priority 1: triangulated pattern
    if detected_pattern and detected_pattern.confidence >= 70:
        email = build_email(detected_pattern.pattern_name, first, last, domain)
        if email and email not in seen:
            candidates.append({
                "email": email,
                "pattern": detected_pattern.pattern_name,
                "source": "detected_pattern",
                "base_confidence": detected_pattern.confidence,
            })
            seen.add(email)

    # Priority 2: first.last@ fallback
    fl = build_email("first.last", first, last, domain)
    if fl and fl not in seen:
        candidates.append({
            "email": fl,
            "pattern": "first.last",
            "source": "first_last_fallback",
            "base_confidence": 45,
        })
        seen.add(fl)

    # Priority 3: industry priors (exclude single-name patterns)
    priors = get_patterns_for(industry)
    filtered_priors = [(p, w) for p, w in priors if p != "first"]
    for pattern_name, weight in filtered_priors:
        if len(candidates) >= 4:
            break
        email = build_email(pattern_name, first, last, domain)
        if email and email not in seen:
            candidates.append({
                "email": email,
                "pattern": pattern_name,
                "source": "industry_prior",
                "base_confidence": int(weight * 50),
            })
            seen.add(email)

    return candidates[:4]


# ============================================================
# SMTP PROBE (Agent 5)
# ============================================================

def _probe_smtp(email: str) -> dict:
    try:
        result = verify_smtp(email, timeout=8)
        return {
            "valid": result.get("status") == "valid",
            "catchall": bool(result.get("is_catchall") or result.get("catchall")),
        }
    except Exception:
        return {"valid": False, "catchall": False}


# ============================================================
# CONFIDENCE SCORING
# ============================================================

def _candidate_confidence(candidate: dict, detected_pattern: Optional[DetectedPattern]) -> int:
    score = candidate.get("base_confidence", 30)

    # NeverBounce is the strongest signal
    if candidate.get("nb_valid"):
        score += 35
    elif candidate.get("nb_result") == "invalid":
        return 0
    elif candidate.get("nb_result") == "catchall":
        score += 5

    # SMTP signals
    if candidate.get("smtp_valid") and not candidate.get("smtp_catchall"):
        score += 20
    if candidate.get("smtp_catchall"):
        score -= 10

    # Boost when the candidate uses the triangulated pattern
    if detected_pattern and candidate.get("pattern") == detected_pattern.pattern_name:
        score += 15

    return max(0, min(100, score))


# ============================================================
# DECISION-MAKER SELECTION
# ============================================================

def _match_hint_to_provider(
    hint: str,
    providers: List[ProviderLookup],
) -> Optional[ProviderLookup]:
    """
    Match a website-scraped contact hint against the NPI provider list.

    If the hint matches an NPI provider by last name (preferred) or first
    name, return that provider. Otherwise build a SYNTHETIC provider from
    the hint itself — the website-scraped name is usually the real owner
    even when NPI lists other providers at the same address.
    """
    if not hint:
        return providers[0] if providers else None

    hint_lower = hint.lower()
    # Word-boundary match — avoids false positives like "an" inside "hasan"
    hint_tokens = set(re.findall(r"[a-z]+", hint_lower))
    for p in providers:
        if p.last_name and len(p.last_name) >= 3 and p.last_name.lower() in hint_tokens:
            return p
    for p in providers:
        if p.first_name and len(p.first_name) >= 3 and p.first_name.lower() in hint_tokens:
            return p

    # No match — synthesize a provider from the hint itself
    synthetic = _synthetic_provider_from_hint(hint)
    if synthetic:
        return synthetic
    return providers[0] if providers else None


_HINT_TITLE_RE = re.compile(
    r"^(dr\.?|doctor|mr\.?|ms\.?|mrs\.?|dds|dmd|md|do|phd|dvm)\b",
    re.IGNORECASE,
)
_HINT_CREDENTIAL_RE = re.compile(
    r",?\s*(dds|dmd|md|do|phd|dvm|esq\.?)\s*$", re.IGNORECASE
)


def _synthetic_provider_from_hint(hint: str) -> Optional[ProviderLookup]:
    """Parse a contact_name hint like 'Dr. Hasan Dbouk' into a ProviderLookup."""
    clean = hint.strip()
    clean = _HINT_CREDENTIAL_RE.sub("", clean).strip()
    # Strip the title prefix (Dr., Mrs., DMD, etc.)
    while True:
        m = _HINT_TITLE_RE.match(clean)
        if not m:
            break
        clean = clean[m.end():].lstrip(" .,").strip()
    tokens = [t for t in re.split(r"\s+", clean) if t]
    if len(tokens) < 2:
        return None
    first_token = re.sub(r"[^A-Za-z]", "", tokens[0])
    last_token = re.sub(r"[^A-Za-z]", "", tokens[-1])
    if not first_token or not last_token:
        return None
    return ProviderLookup(
        full_name=f"{tokens[0]} {tokens[-1]}",
        first_name=first_token,
        last_name=last_token,
        credential=None,
        npi=None,
        source="website_hint",
    )


def _pick_decision_maker(
    providers: List[ProviderLookup],
    business_name: str,
) -> Optional[ProviderLookup]:
    if not providers:
        return None
    practice_clean = re.sub(r"[^a-z]", "", (business_name or "").lower())
    scored = []
    for p in providers:
        score = 0
        last_clean = re.sub(r"[^a-z]", "", p.last_name.lower())
        if last_clean and len(last_clean) >= 3 and last_clean in practice_clean:
            score += 50
        cred = (p.credential or "").upper()
        if any(c in cred for c in ("DDS", "DMD", "MD", "DO")):
            score += 10
        if any(c in cred for c in ("RDH", "RDA", "DA ", "ASSISTANT", "HYGIENIST")):
            score -= 30
        scored.append((p, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[0][0]


# ============================================================
# EVIDENCE
# ============================================================

def _build_evidence(candidate: dict, result: TriangulationResult) -> List[str]:
    out: List[str] = []
    detected = result.detected_pattern

    if detected and candidate.get("pattern") == detected.pattern_name:
        names = ", ".join(detected.evidence_names[:2])
        vote_count = len(detected.evidence_names)
        out.append(
            f"✅ Pattern '{candidate['pattern']}' triangulated from {vote_count} provider(s): {names}"
        )
    elif candidate.get("source") == "first_last_fallback":
        out.append("ℹ️ Default pattern: first.last@ (B2B prior for 10+ employee practices)")
    elif candidate.get("source") == "industry_prior":
        cred = result.decision_maker.credential if result.decision_maker else "healthcare"
        out.append(f"ℹ️ Industry prior pattern for {cred}")

    if candidate.get("nb_valid"):
        out.append("✅ NeverBounce: VALID")
    elif candidate.get("nb_result") == "catchall":
        out.append("⚠️ NeverBounce: CATCH-ALL (domain accepts all mail)")
    elif candidate.get("nb_result") == "invalid":
        out.append("❌ NeverBounce: INVALID")

    if candidate.get("smtp_valid") and not candidate.get("smtp_catchall"):
        out.append("✅ SMTP RCPT accepted")
    elif candidate.get("smtp_catchall"):
        out.append("⚠️ SMTP: catch-all domain")

    return out


# ============================================================
# CONVENIENCE HELPER
# ============================================================

def domain_from_website(website: str) -> str:
    """Normalize a website URL into a bare domain."""
    if not website:
        return ""
    url = website.strip()
    if not url.startswith("http"):
        url = "https://" + url
    try:
        netloc = urlparse(url).netloc
    except Exception:
        return ""
    return (netloc or "").lower().replace("www.", "")
