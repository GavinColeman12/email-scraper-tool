"""
Deep email research pipeline.

Runs 4 research agents in parallel (website, schema.org, linkedin, press),
then feeds the findings to a Claude Sonnet synthesizer that picks the
single best contact with reasoning.

Falls back gracefully: if no Anthropic key is available, uses a rules-based
synthesizer that picks by source-quality heuristics.

Usage:
  from src.deep_scraper import deep_scrape_business_emails
  result = deep_scrape_business_emails(
      business_name="Example Dental", website="https://example.com",
      location="Brooklyn",
  )
  # result is a superset of the dict returned by scrape_business_emails —
  # same keys plus: agent_findings, synthesis_reasoning, email_candidates
"""
import json
import re

from src.email_scraper import (
    scrape_business_emails,
    _normalize_url, _extract_domain, _fetch, CONTACT_PATHS,
    _detect_email_pattern, _build_email_from_pattern, GENERIC_PREFIXES,
)
from src.research_agents import run_all_agents
from src.email_verifier import verify_mx, STATUS_VALID
from src.secrets import get_secret


CLAUDE_MODEL = "claude-sonnet-4-20250514"


SYNTHESIZER_SYSTEM = """You are an expert lead-qualification analyst picking the single best email contact for cold outreach to a decision maker.

You will receive:
- Business name, website, domain
- Findings from 4 research agents: website (team/about pages), schema.org (JSON-LD structured data), linkedin (names from Google's LinkedIn snippets), press (news mentions)
- Already-scraped emails from the website
- The email pattern detected from scraped emails (e.g. "first.last" or "first")

Your job:
1. Identify the highest-authority decision maker across all findings (preference order: Founder/Owner/CEO > President/Managing Partner > Director/VP > Senior Manager)
2. If a name appears in 2+ independent sources (e.g. website AND LinkedIn, OR press AND schema), that's very high confidence
3. Construct the email using the detected pattern + domain. If no pattern, default to firstname@domain
4. Assign confidence:
   - "high": named personal email scraped directly from site, OR name cross-verified across 2+ independent sources
   - "medium": named by 1 source (team page, LinkedIn, or press) and plausibly in charge
   - "low": no named person found, or the only match is a generic role (manager, coordinator)
5. Prefer a constructed named email over a generic inbox (info@, hello@) even at lower confidence — it's better to guess a real person than mail a dead inbox.

Return ONLY a JSON object with these keys (no preamble, no markdown fences):
{
  "contact_name": "Full Name" or "",
  "contact_title": "Title" or "",
  "contact_email": "person@domain.com" or "",
  "confidence": "high" | "medium" | "low",
  "email_source": "short description of how this email was derived",
  "reasoning": "1-2 sentence explanation of WHY this is the best contact — cite which sources confirmed the person",
  "alternate_candidates": [{"name": "...", "email": "...", "source": "..."}, ...]
}"""


def _synthesize_with_claude(business_name, website, domain,
                             agent_findings, scraped_emails,
                             email_pattern) -> dict:
    """Call Claude Sonnet to pick the top contact."""
    try:
        api_key = get_secret("ANTHROPIC_API_KEY")
    except Exception:
        return None
    if not api_key:
        return None

    try:
        import anthropic
    except ImportError:
        return None

    # Build compact findings summary for the prompt
    findings_summary = {}
    for source, data in agent_findings.items():
        people = data.get("people", [])[:5]
        findings_summary[source] = [
            {k: p.get(k, "") for k in ("name", "first", "last", "title", "email")
             if p.get(k)}
            for p in people
        ]

    user_prompt = f"""Business: {business_name}
Website: {website}
Domain: {domain}
Email pattern detected from scraped emails: {email_pattern or "(none — default to first@)"}

Scraped emails ({len(scraped_emails)}):
{json.dumps(scraped_emails[:10], indent=2)}

Agent findings:
{json.dumps(findings_summary, indent=2)}

Pick the best single decision-maker email for cold outreach."""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=800,
            system=SYNTHESIZER_SYSTEM,
            messages=[{"role": "user", "content": user_prompt}],
        )
        text = response.content[0].text if response.content else ""
    except Exception as e:
        return {"_error": str(e)}

    # Extract JSON from response
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        parsed = json.loads(text)
        return parsed
    except Exception:
        # Try to find the JSON block
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
    return None


def _synthesize_rules_based(business_name, website, domain,
                             agent_findings, scraped_emails,
                             email_pattern) -> dict:
    """Fallback synthesizer when Claude isn't available. Uses simple priority rules."""
    def build_email(first, last):
        if email_pattern:
            return _build_email_from_pattern(first, last, domain, email_pattern)
        return f"{(first or '').lower()}@{domain}" if first else ""

    # Collect all people across sources
    all_people = []
    for source, data in agent_findings.items():
        for p in data.get("people", []):
            p = dict(p)
            p["_source"] = source
            all_people.append(p)

    # Count how many sources each name appears in
    name_sources = {}
    for p in all_people:
        key = p.get("name", "").lower()
        if key:
            name_sources.setdefault(key, set()).add(p.get("_source", ""))

    # Priority: cross-verified decision maker > single-source decision maker > any person
    scored = []
    for p in all_people:
        key = p.get("name", "").lower()
        if not key:
            continue
        n_sources = len(name_sources.get(key, set()))
        is_dm = p.get("is_decision_maker", False)
        has_email = bool(p.get("email"))
        score = 0
        if has_email:
            score += 100
        if is_dm:
            score += 50
        if n_sources >= 2:
            score += 30
        # Source quality
        score += {"website": 10, "schema": 15, "linkedin": 8, "press": 12}.get(
            p.get("_source", ""), 0)
        # Prefer team pages specifically
        if p.get("_source") == "website" and "/team" in (p.get("source", "") or ""):
            score += 5
        scored.append((score, p))

    scored.sort(key=lambda x: -x[0])

    # Pick named scraped email first if high-signal
    for e in scraped_emails:
        local = e.partition("@")[0].lower().split(".")[0]
        if local and local not in GENERIC_PREFIXES:
            return {
                "contact_name": scored[0][1].get("name", "") if scored else "",
                "contact_title": scored[0][1].get("title", "") if scored else "",
                "contact_email": e,
                "confidence": "high",
                "email_source": "scraped_personal_email",
                "reasoning": "Personal email scraped directly from website — no guessing required.",
                "alternate_candidates": [],
            }

    # Otherwise use top-scored person
    if scored:
        top_score, top = scored[0]
        email = top.get("email", "") or build_email(top.get("first", ""), top.get("last", ""))
        n_srcs = len(name_sources.get(top.get("name", "").lower(), set()))
        confidence = "high" if n_srcs >= 2 else ("medium" if top.get("is_decision_maker") else "low")
        return {
            "contact_name": top.get("name", ""),
            "contact_title": top.get("title", ""),
            "contact_email": email,
            "confidence": confidence,
            "email_source": f"{top.get('_source', 'unknown')}_cross_verified" if n_srcs >= 2 else top.get("_source", "unknown"),
            "reasoning": f"Found by {n_srcs} source(s): {', '.join(sorted(name_sources.get(top.get('name', '').lower(), set())))}. Constructed with pattern '{email_pattern or 'first'}'.",
            "alternate_candidates": [
                {"name": p.get("name", ""), "email": p.get("email", "") or build_email(p.get("first", ""), p.get("last", "")),
                 "source": p.get("_source", "")}
                for _, p in scored[1:4]
            ],
        }

    # Fallback: generic inbox
    fallback = scraped_emails[0] if scraped_emails else f"info@{domain}"
    return {
        "contact_name": "",
        "contact_title": "",
        "contact_email": fallback,
        "confidence": "low",
        "email_source": "generic_fallback",
        "reasoning": "No named decision maker found via any source. Using generic inbox.",
        "alternate_candidates": [],
    }


def deep_scrape_business_emails(business_name: str, website: str,
                                 location: str = "",
                                 verify_with_mx: bool = True) -> dict:
    """
    Run deep multi-agent research to find the best decision-maker email.

    Steps:
    1. Base scrape (fetches website pages, extracts emails/names, LinkedIn)
    2. Run all research agents in parallel (reusing fetched pages)
    3. Claude synthesizer (or rules fallback) picks top contact
    4. Optional MX verification on the final pick
    """
    # Start with base scrape result — gives us fetched pages, scraped emails,
    # domain, pattern detection, etc.
    base = scrape_business_emails(
        business_name=business_name,
        website=website,
        find_decision_makers=True,
        location=location,
    )
    base["agent_findings"] = {}
    base["synthesis_reasoning"] = ""
    base["email_candidates"] = []

    domain = base.get("domain", "") or _extract_domain(website)
    if not domain:
        return base

    # Re-fetch pages (cheap if cached) so agents can parse them
    website_norm = _normalize_url(website)
    fetched_pages = {}
    if website_norm:
        homepage = _fetch(website_norm)
        if homepage:
            fetched_pages[""] = homepage
            # Fetch same candidate paths the base scraper used (team/about/etc.)
            for path in CONTACT_PATHS[:8]:  # limit to avoid extra cost
                if not path:
                    continue
                url = website_norm.rstrip("/") + "/" + path.lstrip("/")
                html = _fetch(url)
                if html:
                    fetched_pages[path] = html

    # Run all agents in parallel
    agent_findings = run_all_agents(
        business_name=business_name,
        website=website_norm,
        domain=domain,
        location=location,
        fetched_pages=fetched_pages,
    )
    base["agent_findings"] = agent_findings

    # Detect email pattern from scraped emails
    email_pattern = _detect_email_pattern(base.get("scraped_emails", []), domain)

    # Synthesize with Claude (fallback to rules)
    synth = _synthesize_with_claude(
        business_name=business_name, website=website_norm, domain=domain,
        agent_findings=agent_findings,
        scraped_emails=base.get("scraped_emails", []),
        email_pattern=email_pattern,
    )
    used_claude = synth is not None and not synth.get("_error")

    if not used_claude:
        synth = _synthesize_rules_based(
            business_name, website_norm, domain, agent_findings,
            base.get("scraped_emails", []), email_pattern,
        )

    # Overwrite base result with synthesizer output
    if synth:
        base["primary_email"] = synth.get("contact_email", "") or base.get("primary_email", "")
        base["contact_name"] = synth.get("contact_name", "") or base.get("contact_name", "")
        base["contact_title"] = synth.get("contact_title", "") or base.get("contact_title", "")
        base["email_source"] = synth.get("email_source", "") or base.get("email_source", "")
        base["confidence"] = synth.get("confidence", "") or base.get("confidence", "")
        base["synthesis_reasoning"] = synth.get("reasoning", "")
        base["email_candidates"] = synth.get("alternate_candidates", [])
        base["synthesizer"] = "claude" if used_claude else "rules"

    # Optional MX verification on the final pick
    if verify_with_mx and base.get("primary_email"):
        mx_result = verify_mx(base["primary_email"])
        base["mx_status"] = mx_result.get("status", "")
        base["mx_reason"] = mx_result.get("reason", "")
        # Downgrade confidence if MX fails
        if mx_result.get("status") != STATUS_VALID and base.get("confidence") == "high":
            base["confidence"] = "medium"
            base["synthesis_reasoning"] += " (MX check did not confirm — downgraded from high to medium)"

    return base
