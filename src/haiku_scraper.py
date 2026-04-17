"""
Claude Haiku fallback functions for the email scraper.

These run CONDITIONALLY — only when rule-based extraction is weak — to
keep costs low. Each function has a strict JSON schema and gracefully
degrades to an empty / identity result if the API is unavailable.

Cost: ~$0.002 per call at Haiku pricing. Triggered ~30% of businesses
in Verified mode, so ~$0.24 per 200 businesses total.
"""
import json
import os
import re
import sys

# Current Haiku model as of April 2026
HAIKU_MODEL = "claude-haiku-4-5"


def _get_anthropic_client():
    """Return an Anthropic client, or None if not available."""
    try:
        from src.secrets import get_secret
        api_key = get_secret("ANTHROPIC_API_KEY")
    except Exception:
        api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        import anthropic
        return anthropic.Anthropic(api_key=api_key)
    except ImportError:
        return None


def _call_haiku(system: str, user: str, max_tokens: int = 1500) -> str:
    """Make a single Haiku call. Returns raw text, or empty on failure."""
    client = _get_anthropic_client()
    if not client:
        return ""
    try:
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return resp.content[0].text if resp.content else ""
    except Exception as e:
        print(f"[haiku_scraper] API error: {type(e).__name__}: {e}", file=sys.stderr)
        return ""


def _parse_json(text: str, fallback):
    """Extract + parse a JSON object or array from Haiku's text response."""
    if not text:
        return fallback
    t = text.strip()
    # Strip markdown fences
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t)
        t = re.sub(r"\s*```$", "", t).strip()
    try:
        return json.loads(t)
    except Exception:
        # Try to find the outermost {} or []
        for open_ch, close_ch in (("{", "}"), ("[", "]")):
            start = t.find(open_ch)
            end = t.rfind(close_ch)
            if start != -1 and end > start:
                try:
                    return json.loads(t[start:end + 1])
                except Exception:
                    continue
        return fallback


# ── Function 1: Extract structured people from a team page ───────────

EXTRACT_PEOPLE_SYSTEM = """You are a precise HTML parser that extracts people from a business team/about page.

You will receive the raw HTML text of a team page. Extract ALL real humans who work at this business (doctors, owners, staff, etc.).

Rules:
- Return ONLY real people with proper first + last names. NO department names, job titles alone, or marketing phrases.
- NEVER include: "Partner Services", "Our Team", "Owner Director", "Customer Service", etc.
- Titles must come from the actual text near the name (Owner, CEO, Founder, Dr., DDS, Principal, Director, etc.)
- Only extract the email if it appears literally on the page tied to that person.
- Skip if you can't find a first AND last name for the person.
- Return at most 10 people."""


def _build_people_user_prompt(html: str, business_name: str) -> str:
    # Strip HTML to plain text to keep the prompt compact
    from bs4 import BeautifulSoup
    try:
        soup = BeautifulSoup(html, "html.parser")
        # Remove script/style noise
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)[:8000]
    except Exception:
        text = html[:8000]

    return f"""Business: {business_name}

Team page text:
{text}

Return a JSON array of people with this exact shape:
[
  {{
    "name": "Full Name",
    "first": "First",
    "last": "Last",
    "title": "Their title at this business",
    "email": "person@domain.com or null if not on page",
    "bio_excerpt": "one sentence capturing why they're important, or empty string"
  }}
]

Return an empty array [] if no real people are listed. Output JSON only, no preamble or code fences."""


def haiku_extract_people(html: str, business_name: str) -> list:
    """
    Parse a team/about page HTML for structured people data.
    Returns a list of {name, first, last, title, email, bio_excerpt}.

    Use when rule-based extraction finds <2 people — this handles
    carousels, image captions, grid cards, and other layouts where
    regex fails.
    """
    if not html or not business_name:
        return []
    user_prompt = _build_people_user_prompt(html, business_name)
    text = _call_haiku(EXTRACT_PEOPLE_SYSTEM, user_prompt, max_tokens=2000)
    parsed = _parse_json(text, [])
    if not isinstance(parsed, list):
        return []

    # Validate + normalize each entry
    cleaned = []
    for p in parsed[:10]:
        if not isinstance(p, dict):
            continue
        name = (p.get("name") or "").strip()
        first = (p.get("first") or "").strip()
        last = (p.get("last") or "").strip()
        if not name or not first:
            continue
        # Sanity: first and last should be alpha-ish, reasonable length
        if not re.match(r"^[A-Za-z][A-Za-z'\-.]+$", first):
            continue
        cleaned.append({
            "name": name,
            "first": first,
            "last": last,
            "title": (p.get("title") or "").strip(),
            "email": (p.get("email") or "").strip().lower() or None,
            "bio_excerpt": (p.get("bio_excerpt") or "").strip(),
        })
    return cleaned


# ── Function 2: Match scraped emails to extracted people ─────────────

MATCH_EMAILS_SYSTEM = """You match email addresses to the people they belong to at a business.

Given a list of scraped emails and a list of people, decide which email (if any) belongs to each person. Be conservative — only confirm a match if the local part of the email clearly corresponds to the person's name."""


def haiku_match_emails_to_people(emails: list, people: list, domain: str) -> dict:
    """
    Given emails + people, return a dict mapping email → person_name
    (or None if the email doesn't clearly belong to any person).

    Use when we have ambiguous prefixes like 'jsmith@' and need to know
    if it's John Smith or Jane Smith.
    """
    if not emails or not people:
        return {}

    user_prompt = f"""Domain: {domain}

Emails: {json.dumps(emails)}

People at this business:
{json.dumps([{'name': p['name'], 'first': p.get('first', ''), 'last': p.get('last', '')} for p in people])}

Return a JSON object mapping each email to a person name (string) or null:
{{"email1@domain.com": "John Smith", "info@domain.com": null, ...}}

Output JSON only. No preamble. Be conservative — use null when unsure."""

    text = _call_haiku(MATCH_EMAILS_SYSTEM, user_prompt, max_tokens=800)
    parsed = _parse_json(text, {})
    if not isinstance(parsed, dict):
        return {}
    # Clean the result — keys must be in our emails list
    return {k.lower(): v for k, v in parsed.items()
            if isinstance(k, str) and k.lower() in {e.lower() for e in emails}}


# ── Function 3: Filter obvious false-positive emails ─────────────────

FILTER_FALSE_POSITIVES_SYSTEM = """You filter out obvious false-positive email addresses from a scraped list.

Remove emails that are clearly not real business contacts:
- File artifacts: anything@2x.png, sprite-xyz@, logo-hash@
- Placeholder / template: your-name@, example@, test@, foo@
- Code mentions: variable names misparsed as emails
- Marketing/tracking IDs embedded in a URL as an email

Keep everything that could plausibly be a real business email, even generic ones like info@ or sales@."""


def haiku_filter_false_positives(emails: list) -> list:
    """
    Given a list of candidate emails, return only the ones that look like
    real business contacts. Removes obvious junk.

    Use when scraping produced ≥5 candidates (signals noisy page).
    """
    if not emails:
        return []
    if len(emails) < 2:
        return emails  # not worth a Haiku call for 1 email

    user_prompt = f"""Scraped candidate emails:
{json.dumps(emails)}

Return a JSON array with only the REAL business emails kept. Output JSON only, no preamble."""

    text = _call_haiku(FILTER_FALSE_POSITIVES_SYSTEM, user_prompt, max_tokens=800)
    parsed = _parse_json(text, emails)
    if not isinstance(parsed, list):
        return emails
    # Never return MORE than the input
    kept = [e for e in parsed if isinstance(e, str) and e.lower() in
             {orig.lower() for orig in emails}]
    return kept or emails  # if Haiku returned empty, keep original


# ── Function 4: Cross-reference conflicting signals ──────────────────

CROSS_REF_SYSTEM = """You consolidate conflicting information about a person from multiple sources.

Given a person's name plus snippets from LinkedIn, website, and press sources, return a single consolidated view with the most trustworthy title and role at the business."""


def haiku_cross_reference(name: str, linkedin_snippet: str = "",
                           press_snippet: str = "",
                           website_snippet: str = "") -> dict:
    """
    Reconcile conflicting info about a person across sources.
    Returns {name, title, is_decision_maker, reasoning}.

    Use when same person appears in ≥2 sources with different titles.
    """
    if not name:
        return {}

    sources_text = ""
    if linkedin_snippet:
        sources_text += f"LinkedIn: {linkedin_snippet}\n"
    if press_snippet:
        sources_text += f"Press/News: {press_snippet}\n"
    if website_snippet:
        sources_text += f"Website: {website_snippet}\n"

    if not sources_text:
        return {"name": name, "title": "", "is_decision_maker": False}

    user_prompt = f"""Person: {name}

Sources:
{sources_text}

Return a JSON object:
{{
  "name": "Full Name",
  "title": "their consolidated title",
  "is_decision_maker": true or false,
  "reasoning": "one sentence explaining your consolidation"
}}

Output JSON only."""

    text = _call_haiku(CROSS_REF_SYSTEM, user_prompt, max_tokens=400)
    parsed = _parse_json(text, {})
    if not isinstance(parsed, dict):
        return {"name": name, "title": "", "is_decision_maker": False}
    return {
        "name": parsed.get("name", name),
        "title": (parsed.get("title") or "").strip(),
        "is_decision_maker": bool(parsed.get("is_decision_maker", False)),
        "reasoning": (parsed.get("reasoning") or "").strip(),
    }


# ── Availability check ───────────────────────────────────────────────

def is_haiku_available() -> bool:
    """Return True if Haiku is callable (API key present + SDK installed)."""
    return _get_anthropic_client() is not None
