"""
Haiku-backed final email picker.

Replaces the stopword treadmill with LLM judgment: give Haiku the DM's
name, the business, and all candidate emails — it picks the one most
likely to reach the DM, or says NONE if they all look bad.

One Haiku call per business. Cached by (dm_name, business, domain,
hash_of_candidates) so re-runs pay $0.

Why not just rules:
  Our stopword list is forever incomplete. Every new industry adds
  new shared-inbox aliases (catering@, specialevents@, caseintake@,
  freeconsult@). Haiku judges semantically — if the email looks like
  a shared/marketing inbox, it rejects; if it looks like the DM's
  actual mailbox, it picks; if it's a DIFFERENT person at the firm
  (jake@firm.com when DM is Matthew Weaver), it also rejects.

Budget:
  ~$0.001-0.002 per business at Haiku pricing. Runs once per biz.
  At 200 biz that's ~$0.30. Cached — re-runs are free.

Fallback:
  If Haiku is unavailable (no API key, API error, malformed output),
  return None. Caller falls through to the rule-based ranking walker.

Public API:
  pick_email_with_llm(candidates, dm, business_name, domain, cache)
    -> (picked_email, reason) | (None, reason)  when Haiku says none OK
    -> None                                      on Haiku failure / no key
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


def _parse_json_obj(text: str):
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


def _cache_key(dm_name: str, business_name: str, domain: str,
               candidates: list) -> tuple:
    emails = sorted((c.get("email") or "").lower() for c in candidates)
    raw = (f"{dm_name.lower()}||{business_name.lower()}||{domain.lower()}"
           f"||{'|'.join(emails)}")
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return ("llm_email_pick", h)


def pick_email_with_llm(
    candidates: list,
    dm_name: str,
    dm_title: str,
    business_name: str,
    domain: str,
    cache,
):
    """
    Ask Haiku to pick the candidate most likely to reach the DM.

    `candidates` is a list of dicts with at least:
      {email, bucket, pattern, nb_result}

    Returns:
      (email_str, reason_str)            if Haiku picks one
      (None, reason_str)                 if Haiku says none are good
      None                               if Haiku is unavailable / errored

    The ("" / None) case is meaningful — it signals "every candidate
    looks like a shared inbox or a different person". The caller
    should mark the row as volume_empty or volume_review rather than
    falling back to rule-based ranking (which picked the wrong thing).
    """
    if not candidates:
        return (None, "no candidates")

    key = _cache_key(dm_name, business_name, domain, candidates)
    try:
        cached = cache.get(*key)
    except Exception:
        cached = None
    if cached is not None:
        # cached is (email_or_none, reason)
        try:
            e, r = cached
            return (e, r)
        except Exception:
            pass

    client = _get_client()
    if client is None:
        return None

    # Normalize candidate rows for the prompt
    items = []
    for i, c in enumerate(candidates):
        items.append({
            "i": i,
            "email": c.get("email") or "",
            "bucket": c.get("bucket") or "",
            "pattern": c.get("pattern") or "",
            "nb": c.get("nb_result") or "untested",
        })

    system_prompt = (
        "You are the final gate that decides which email address a cold "
        "outreach message should go to. You are given the decision-maker "
        "(DM) we want to reach, the business name, and a list of candidate "
        "emails the scraper found or guessed for this business.\n\n"
        "Pick the ONE candidate most likely to land in the DM's personal "
        "inbox, OR pick NONE if every candidate is unsuitable.\n\n"
        "A good pick:\n"
        "  - Local part contains the DM's first name, last name, or common "
        "initials (e.g. 'jrb' for 'John R. Buhrman'). 9 out of 10 real "
        "personal emails contain the person's name somehow.\n"
        "  - NB verdict is 'valid' (strongly preferred) or 'untested'.\n"
        "  - Matches a pattern consistent with the business's naming "
        "convention.\n\n"
        "REJECT (pick NONE or skip to next candidate):\n"
        "  - Generic/shared inboxes. Entire categories, not an exhaustive "
        "list — use judgment to recognize new variants:\n"
        "    * Contact / info: info@, contact@, hello@, hi@, welcome@\n"
        "    * Engagement / \"start a conversation\": connect@, reach@, "
        "reachout@, touch@, chat@, meet@, letstalk@, talktous@, "
        "workwithus@, engageus@, inquire@\n"
        "    * Sales / support: sales@, support@, service@, help@\n"
        "    * Admin / team: admin@, office@, team@, reception@\n"
        "    * Restaurant: catering@, reservations@, management@, "
        "specialevents@, gifts@, kitchen@, orders@\n"
        "    * Law-firm marketing: complimentarycasereview@, "
        "freeconsultation@, caseintake@, newclient@\n"
        "    * Accessibility / compliance: accessibility@, ada@, a11y@\n"
        "    * Venue / location aliases: 233thompson@, 90park@, "
        "felice56@, barfelice@, gct@\n"
        "  These are routed through an admin/receptionist and rarely "
        "reach the DM. If the local part reads like a role, a marketing "
        "funnel, or a \"reach out to us\" phrase rather than a person's "
        "name — reject.\n"
        "  - Emails for a DIFFERENT person at the same firm. Example: "
        "if DM is 'Matthew Weaver' and the candidate is 'jake@firm.com' "
        "or 'katiebrice@firm.com', Jake and Katie are colleagues, not "
        "Matthew. REJECT these — do not pick the wrong person just because "
        "NB says the mailbox exists.\n"
        "  - NB verdict 'invalid' = confirmed bounce. Never pick.\n"
        "  - Location / venue prefixes (street-number@, storenum@, city@).\n\n"
        "EDGE CASES:\n"
        "  - Nickname equivalence: 'chris' for 'Christopher', 'mike' for "
        "'Michael', 'jeff' for 'Jeffrey' — these ARE the DM's name.\n"
        "  - Initials: 'jrb' could match 'John R. Buhrman' if the name "
        "fits. If not, reject.\n"
        "  - First-name-only when the DM's first name is distinctive: "
        "'pascal@domain' for 'Pascal Petiteau' is a good pick.\n"
        "  - When DM has no first/last (only one name or empty), be "
        "conservative — prefer NONE over guessing.\n"
        "  - When the DM field is EMPTY (unknown), we couldn't identify "
        "a specific decision maker. In that case you can still REJECT "
        "every shared/generic inbox candidate — return NONE if that's "
        "all we have. But if a candidate looks like a person's name "
        "(e.g. \"kariss@domain\" or \"mbrady@domain\"), pick it as the "
        "best guess — better than a generic inbox.\n\n"
        "OUTPUT — JSON only, no prose, no markdown:\n"
        '  {"pick": <index or null>, "reason": "<one sentence>"}\n'
        "pick is the `i` of the chosen candidate, or null if NONE fit.\n"
        "reason is a terse explanation (under 20 words) — this goes in "
        "the operator's decision log."
    )

    user_prompt = (
        f"Business: {business_name}\n"
        f"Domain: {domain}\n"
        f"Decision maker: {dm_name}"
        + (f" ({dm_title})" if dm_title else "")
        + "\n\nCandidates:\n"
        f"{json.dumps(items, ensure_ascii=False)}\n\n"
        "Return JSON: {\"pick\": <index or null>, \"reason\": \"...\"}"
    )

    try:
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=200,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = resp.content[0].text if resp.content else ""
    except Exception as e:
        print(f"[email_picker_llm] API error: {type(e).__name__}: {e}",
              file=sys.stderr)
        return None

    obj = _parse_json_obj(raw)
    if not isinstance(obj, dict):
        return None

    pick = obj.get("pick")
    reason = (obj.get("reason") or "").strip()[:200]

    chosen_email = None
    if pick is not None:
        try:
            idx = int(pick)
            if 0 <= idx < len(candidates):
                chosen_email = candidates[idx].get("email") or ""
        except Exception:
            chosen_email = None

    # Guard: if Haiku picked an email that would fail our rule-based
    # generic filter, trust the rule (belt + suspenders). Haiku
    # generally agrees but this keeps catastrophic misfires bounded.
    if chosen_email:
        try:
            from src.volume_mode.stopwords import email_is_generic
            if email_is_generic(chosen_email, business_name=business_name):
                chosen_email = None
                reason = (reason + " | rule-overridden: local is generic").strip()[:200]
        except Exception:
            pass

    result = (chosen_email, reason or "llm pick")

    try:
        cache.set(key[0], list(result), key[1])
    except Exception:
        pass

    return result
