"""
LLM-based name classifier — replaces the stopword-list treadmill.

Problem it solves:
  The name-extraction regex pattern-matches "two capitalized words", which
  is structurally correct for person names but also matches every SEO
  phrase on a law firm's /about page ("Call Now", "See All", "Premises
  Liability", "North Mopac"). The stopword list grew with every new
  vertical we scraped and never stopped leaking.

Strategy:
  One Haiku call per business. Takes the full list of extracted
  OwnerCandidate names, classifies each as real_person vs not_person
  given the business context, returns only the real people.

Cost / latency:
  - ~$0.001-0.002 per business at Haiku pricing (well under the $0.05
    triangulation budget)
  - ~300-600ms per call (one call per business regardless of list size)
  - Cached per (business_name, domain, hash_of_candidate_names) for 30
    days so reruns pay $0

Fallback behavior:
  If ANTHROPIC_API_KEY is missing, the Anthropic SDK import fails, or
  the API call errors, we return None. Callers fall back to
  _is_junk_name (stopword-based filter). No breaking changes.

Public API:
  filter_real_people(candidates, business_name, domain, cache) -> list
    Returns a new list containing only the OwnerCandidates Haiku flagged
    as real people. Returns None on any failure — callers should treat
    None as "skip this filter" and use the stopword fallback.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys


HAIKU_MODEL = "claude-haiku-4-5"


def _get_client():
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


def _parse_json_array(text: str):
    if not text:
        return None
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t)
        t = re.sub(r"\s*```$", "", t).strip()
    try:
        return json.loads(t)
    except Exception:
        return None


def _candidate_cache_key(candidates: list, business_name: str, domain: str) -> tuple:
    """Stable cache key — so the same candidate list on a re-run hits cache."""
    names = sorted((c.full_name or "").lower() for c in candidates)
    raw = "|".join(names) + f"||{business_name.lower()}||{domain.lower()}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return ("llm_name_filter", h)


def filter_real_people(
    candidates: list,
    business_name: str,
    domain: str,
    cache,
):
    """
    Return a filtered list of OwnerCandidates containing only real people,
    OR return None if the classifier is unavailable (caller falls back to
    stopword filter).

    Input candidates should be OwnerCandidate-like objects with attributes:
      full_name, first_name, last_name, title, source

    When the same list is submitted again (by name set + biz + domain),
    the cached classification is returned — no re-spend.
    """
    if not candidates:
        return []

    # Cache check first — no API cost on re-runs
    cache_key = _candidate_cache_key(candidates, business_name, domain)
    try:
        cached = cache.get(*cache_key)
    except Exception:
        cached = None
    if cached is not None:
        # cached is the lowercase set of names classified as real people
        keep = set(cached)
        return [c for c in candidates
                if (c.full_name or "").lower() in keep]

    # Call Haiku
    client = _get_client()
    if client is None:
        # No API key — signal fallback
        return None

    # Build the prompt
    items = []
    for i, c in enumerate(candidates):
        item = {
            "i": i,
            "name": c.full_name,
        }
        # Title is useful context but optional
        title = getattr(c, "title", "") or ""
        if title:
            item["title"] = title
        items.append(item)

    system_prompt = (
        "You are a filter that classifies candidate strings as real-person "
        "names or not. These candidates were extracted by regex from web "
        "pages and search snippets — the text is noisy and includes SEO "
        "copy, UI fragments, city names, and marketing phrases alongside "
        "actual human names.\n\n"
        "A real person has:\n"
        "  - A plausible first name (common given name — any culture)\n"
        "  - A plausible last name (surname — not a common English "
        "noun/verb/adjective/phrase)\n\n"
        "Reject anything that is clearly NOT a person:\n"
        "  - SEO phrases: 'Call Now', 'See All', 'Read More', "
        "'Free Consultations'\n"
        "  - City/place names: 'San Marcos', 'North Mopac', 'Travis County'\n"
        "  - Business descriptors: 'Premises Liability', 'Car Accident', "
        "'Client Testimonials'\n"
        "  - Sentence fragments: 'Get Stephen', 'Named Stephen', "
        "'Already Started'\n"
        "  - Business name fragments: 'Spodek Law' at 'Spodek Law Group'\n"
        "  - Role titles standalone: 'Executive Director', 'Chief Executive'\n\n"
        "Keep anything that IS a plausible person name, even if unusual or "
        "non-Anglo. Prefer false-negatives (discarding a real name) over "
        "false-positives (keeping a non-name). When uncertain, reject.\n\n"
        "Return ONLY a JSON array of integer indices (the `i` field) of "
        "candidates that ARE real people. No prose, no explanation, no "
        "markdown fences.\n\n"
        "Example input:\n"
        '  [{"i":0,"name":"Call Now","title":"Owner"},\n'
        '   {"i":1,"name":"Blake Quackenbush","title":"Founder"},\n'
        '   {"i":2,"name":"Premises Liability","title":"Partner"}]\n'
        "Example output:\n"
        "  [1]\n"
    )

    user_prompt = (
        f"Business: {business_name}\n"
        f"Domain: {domain}\n\n"
        f"Candidates:\n{json.dumps(items, ensure_ascii=False)}\n\n"
        f"Return the JSON array of real-person indices:"
    )

    try:
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=500,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = resp.content[0].text if resp.content else ""
    except Exception as e:
        print(f"[name_classifier] API error: {type(e).__name__}: {e}",
              file=sys.stderr)
        return None

    indices = _parse_json_array(raw)
    if not isinstance(indices, list):
        # Bad output — fall back to stopword filter
        return None

    valid_indices = set()
    for x in indices:
        try:
            valid_indices.add(int(x))
        except Exception:
            continue

    kept = [c for i, c in enumerate(candidates) if i in valid_indices]

    # Cache the lowercased-name set so reruns skip the Haiku call
    try:
        cache.set(
            cache_key[0],
            [(c.full_name or "").lower() for c in kept],
            cache_key[1],
        )
    except Exception:
        pass

    return kept
