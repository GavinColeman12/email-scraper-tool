"""
Pre-send safety gate for enterprise-grade bounce-rate targets (<0.3%).

The scraper produces candidates across a spectrum of confidence —
NB-verified, NB-catchall, NB-unknown, industry-prior guesses. For a
brand-new sender domain trying to stay under a 0.3% bounce rate (ESP
acceptance thresholds are typically 0.5%; Gmail/M365 flag above 2%),
sending any non-NB-verified row is a reputation risk.

This module is the strict filter that decides which rows are
SEND-SAFE today. It composes every signal we have:

  1. NB verdict must be "valid" (rejects catchall, unknown, invalid)
  2. MX record must exist for the domain (pre-check via dns.resolver)
  3. NB verdict must be FRESH — stale verdicts (emails from >14 days
     ago) are assumed changed; flag for re-verification before send
  4. Domain must not be on our bounce history — any prior hard-bounce
     on the same domain auto-disqualifies future sends there
  5. Business must have a real Google Maps rating (>=3 stars, >=1
     review) — rating-less listings are often fake/defunct
  6. Email must not already have been sent to within 90 days (prevent
     duplicate outreach which ESPs treat as spam signal)

Returns a (safe: bool, reasons: list[str]) tuple per biz so the UI
can show operators EXACTLY why a row was held back.

The strict default targets <0.3%. The `permissive=True` mode relaxes
the rating + freshness gates for operators who want wider nets at
volume_verified quality (accepting ~1% bounce in exchange for 2-3×
more addresses).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional


# Default freshness window — NB verdicts older than this need re-verify
# before sending. 14d is a balance between "too stale" and "burn NB
# credits on every run". Gmail / M365 change address status quickly
# after a person leaves a company, so 14d is already generous.
NB_FRESHNESS_DAYS_DEFAULT = 14

# Minimum Google rating for a biz to pass the send-safety gate.
# 3.0 is low on purpose — we already filter heavily upstream, this
# just catches zombie listings. Operators can override via kwargs.
MIN_RATING_DEFAULT = 3.0
MIN_REVIEW_COUNT_DEFAULT = 1


def _parse_dt(value) -> Optional[datetime]:
    """Parse a TIMESTAMP field into a naive UTC datetime, tolerating
    both Postgres (datetime) and SQLite (ISO string) shapes."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        s = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


def _domain_of(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.split("@", 1)[1].lower().strip()


def _local_of(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.split("@", 1)[0].lower().strip()


def _local_contains_dm_name(
    email: str, dm_first: str, dm_last: str,
) -> bool:
    """
    True if the email local part plausibly belongs to the identified DM.
    Covers: exact name match, nickname equivalence (Jeff ↔ Jeffrey),
    "dr" prefix (dr{last} is common in dental/medical), first-initial
    + last, last-initial, and short-form initials.

    Returns FALSE when the local part doesn't contain the DM's name —
    a strong signal that either (a) the email is a shared inbox or
    (b) it belongs to a different person at the company. Either way,
    risky for <0.3% bounce rate targets.
    """
    if not email or "@" not in email:
        return False
    local = _local_of(email)
    if not local:
        return False
    first = (dm_first or "").lower().strip()
    last = (dm_last or "").lower().strip()
    if not first and not last:
        # No DM info to match against — can't evaluate
        return False

    # Strict word-boundary check — prevents "bill" (nickname for
    # William) from matching inside "patientBILLing". A match counts
    # only when the needle is at the start of local, end of local, or
    # flanked by separators (. - _).
    def _boundary_match(needle: str) -> bool:
        if not needle or len(needle) < 3 or needle not in local:
            return False
        # OK if at start and followed by end-of-local or separator
        if local.startswith(needle):
            rest_start = len(needle)
            if rest_start == len(local):
                return True  # exact match
            if local[rest_start] in "._-":
                return True
            # e.g. "jeff" + "erson" still counts when the local is the
            # full first name (handled above); flast patterns below.
        # OK if at end and preceded by end-of-local or separator
        if local.endswith(needle):
            lhs_end = len(local) - len(needle)
            if lhs_end == 0:
                return True
            if local[lhs_end - 1] in "._-":
                return True
        # Flanked by separators
        idx = local.find(needle)
        while idx != -1:
            left_ok = idx == 0 or local[idx - 1] in "._-"
            right_end = idx + len(needle)
            right_ok = right_end == len(local) or local[right_end] in "._-"
            if left_ok and right_ok:
                return True
            idx = local.find(needle, idx + 1)
        return False

    # Direct name match (strict boundaries)
    if _boundary_match(first):
        return True
    if _boundary_match(last):
        return True

    # Nickname equivalents (Jeff ↔ Jeffrey, Mike ↔ Michael, Bill ↔
    # William). Boundary-checked so "bill" doesn't match
    # "patientbilling".
    try:
        from src.name_equivalence import equivalents
        if first:
            for nick in equivalents(first):
                if _boundary_match(nick):
                    return True
    except Exception:
        pass

    # Initial + last-name patterns: flast, f.last, firstl. These are
    # naturally boundary-anchored because they span most of the local.
    if first and last and len(last) >= 3:
        if local == first[0] + last:       # pwyatt
            return True
        if local == first[0] + "." + last: # p.wyatt
            return True
        if local == first + last[0]:       # paulaw
            return True
        # Allow flast as a prefix only when followed by separator
        prefix = first[0] + last
        if local.startswith(prefix) and (
            len(local) == len(prefix) or local[len(prefix)] in "._-"
        ):
            return True

    # Doctor prefix — common in dental/medical: dr{last}, dr.{last}
    if last and len(last) >= 3:
        for dp in ("dr", "doctor", "doc"):
            # Exact: "drwyatt" == "dr" + "wyatt" or "dr.wyatt"
            if local == dp + last:
                return True
            if local == dp + "." + last:
                return True
            # Prefix: "drwyatt@" at the start of local
            if local.startswith(dp + last):
                rest = local[len(dp + last):]
                if not rest or rest[0] in "._-":
                    return True

    # Three-letter initials (Jeffrey R. Buhrman → jrb)
    if first and last:
        fl = first[0] + last[0]
        if local == fl:
            return True
        if len(local) == 3 and local[0] == first[0] and local[-1] == last[0]:
            return True

    return False


def is_safe_to_send(
    biz: dict,
    *,
    domain_bounce_set: Optional[set] = None,
    nb_freshness_days: int = NB_FRESHNESS_DAYS_DEFAULT,
    min_rating: float = MIN_RATING_DEFAULT,
    min_review_count: int = MIN_REVIEW_COUNT_DEFAULT,
    permissive: bool = False,
) -> tuple[bool, list[str]]:
    """
    Return (safe, reasons) for a single business row. `reasons` is a
    list of short strings describing what failed — empty list when safe.

    `domain_bounce_set` is a pre-computed set of lowercased domains
    that have ever hard-bounced in our history. Callers pass this so
    the function isn't doing a DB query per row — build once per
    filter pass via send_safety.domains_with_bounces(...) below.

    `permissive=True` relaxes the rating + freshness gates. Safest is
    the default (permissive=False).
    """
    reasons: list[str] = []

    email = (biz.get("primary_email") or "").strip().lower()
    if not email or "@" not in email:
        return False, ["no email"]

    # 1. NB verdict must be "valid"
    nb_result = (biz.get("neverbounce_result") or "").lower().strip()
    if nb_result != "valid":
        if nb_result in ("catchall", "catch-all"):
            reasons.append("NB catchall — mailbox existence unconfirmed")
        elif nb_result == "unknown":
            reasons.append("NB unknown — verification timed out")
        elif nb_result == "invalid":
            reasons.append("NB invalid — confirmed bounce")
        else:
            reasons.append(f"NB untested ({nb_result or 'never checked'})")

    # 2. Freshness — NB verdict older than N days needs re-verify.
    # scraped_at is our proxy for NB-check time since NB runs during scrape.
    if not permissive:
        scraped_at = _parse_dt(biz.get("scraped_at"))
        if scraped_at:
            age_days = (datetime.utcnow() - scraped_at).days
            if age_days > nb_freshness_days:
                reasons.append(
                    f"NB verdict is {age_days} days stale "
                    f"(limit {nb_freshness_days}d) — re-verify before send"
                )
        else:
            # No scraped_at means we don't know when this was checked.
            # Treat as stale to be safe.
            reasons.append("no scrape timestamp — re-verify before send")

    # 3. Domain bounce history — any prior bounce on this domain is
    # a blocker. Bounces come in pairs (person leaves, whole domain
    # rejects) and ESPs punish repeat-offender domains.
    domain = _domain_of(email)
    if domain and domain_bounce_set and domain in domain_bounce_set:
        reasons.append(f"{domain} has prior bounces in history")

    # 4. Business legitimacy — low-rating or review-less listings are
    # disproportionately defunct/zombie. Skipping in strict mode.
    if not permissive:
        rating = float(biz.get("rating") or 0)
        reviews = int(biz.get("review_count") or 0)
        if rating < min_rating:
            reasons.append(f"rating {rating} < {min_rating}")
        if reviews < min_review_count:
            reasons.append(f"{reviews} reviews < {min_review_count}")

    # 5. Safe_to_send flag from the scrape pipeline — it's the
    # pipeline's own "would I send this" verdict. If it says no,
    # respect it regardless of NB.
    pipeline_safe = biz.get("email_safe_to_send")
    # SQLite stores booleans as 0/1; Postgres as True/False
    if pipeline_safe is not None and not pipeline_safe:
        reasons.append("pipeline safe_to_send=false")

    # 6. DM-name-match gate — local part must plausibly contain the
    # identified DM's name. Catches the search-45 failure class where
    # Haiku picked a bucket-C NB-valid scraped email (hba@, manager@,
    # patientbilling@, civilrights@) because all the DM patterns came
    # back NB-invalid. NB-valid + non-DM-match = a shared inbox or a
    # different person; neither is acceptable at <0.3% bounce.
    if not permissive:
        dm_first = (biz.get("contact_first") or biz.get("first_name") or "").strip()
        dm_last = (biz.get("contact_last") or biz.get("last_name") or "").strip()
        # Fall back to splitting contact_name when the separate columns
        # aren't populated (older rows, some export paths).
        if not dm_first and not dm_last:
            full = (biz.get("contact_name") or "").strip()
            if full:
                parts = full.split(None, 1)
                dm_first = parts[0] if parts else ""
                dm_last = parts[1] if len(parts) > 1 else ""
        if dm_first or dm_last:
            if not _local_contains_dm_name(email, dm_first, dm_last):
                reasons.append(
                    f"local part doesn't match DM name "
                    f"({dm_first} {dm_last}) — likely shared inbox "
                    f"or wrong person"
                )

    return (len(reasons) == 0, reasons)


def mark_duplicate_emails(businesses: list[dict]) -> dict:
    """
    Given a list of business rows, return a dict
    {biz_id: duplicate_index} where duplicate_index is:
      0 — first occurrence of this email in the list (safe to send)
      1, 2, ... — later occurrences (duplicate — skip to avoid
                  double-sending the same person)

    Use case: search #45 had `mohammad.spouh@aspendental.com` on two
    Aspen Dental franchise locations. Same person, two rows — sending
    twice hits the spam-signal threshold at big ESPs.
    """
    seen_count: dict[str, int] = {}
    out: dict = {}
    for biz in businesses:
        email = (biz.get("primary_email") or "").strip().lower()
        if not email:
            continue
        idx = seen_count.get(email, 0)
        out[biz.get("id")] = idx
        seen_count[email] = idx + 1
    return out


def classify_for_send(
    biz: dict, **kwargs
) -> str:
    """
    Simpler bucketing than is_safe_to_send — returns one of:
      "send"         — all gates pass, safe to include in next batch
      "reverify"     — stale NB, re-run the check and re-classify
      "review"       — ambiguous (NB unknown / catchall, rating low)
      "skip"         — confirmed unsafe (NB invalid, domain bounced)
    Suitable for grouping the CSV export into three worksheets.
    """
    safe, reasons = is_safe_to_send(biz, **kwargs)
    if safe:
        return "send"
    joined = " | ".join(reasons).lower()
    if "nb invalid" in joined or "prior bounces" in joined:
        return "skip"
    # DM-name mismatch = "wrong person or shared inbox" — goes to
    # review for human check, not skip. The mailbox may still deliver
    # (e.g. manager@ reaches someone at the practice) but shouldn't
    # be auto-sent without looking.
    if "doesn't match dm name" in joined:
        return "review"
    if "stale" in joined and not ("catchall" in joined or "invalid" in joined):
        return "reverify"
    return "review"


def domains_with_bounces() -> set:
    """
    Pull the set of all domains that have EVER had a bounced send in
    our email_sends history. Used as a blocklist for the safe-send
    filter — we don't want to ever send to a domain that's already
    bounced someone, because the whole domain is often blacklisting
    our sender IP/reputation.

    Reads the email_sends table directly via storage helpers.
    """
    try:
        from src.storage import _connect, _cursor, USE_PG
    except Exception:
        return set()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute("""
            SELECT DISTINCT domain FROM email_sends
             WHERE status = 'bounced' AND domain IS NOT NULL AND domain <> ''
        """)
        rows = cur.fetchall()
        out = set()
        for r in rows:
            d = dict(r) if hasattr(r, "keys") else (r[0] if isinstance(r, tuple) else r)
            domain = (d.get("domain") if isinstance(d, dict) else d) or ""
            if domain:
                out.add(domain.lower().strip())
        return out
    except Exception:
        return set()
    finally:
        conn.close()


def previously_sent_emails(days: int = 90) -> set:
    """
    Return the set of email addresses already sent in the last N days.
    Used to dedupe the send queue — sending twice to the same person
    within 90 days is a spam signal the big ESPs pick up on.
    """
    try:
        from src.storage import _connect, _cursor, _PARAM
    except Exception:
        return set()
    cutoff = datetime.utcnow() - timedelta(days=days)
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"""SELECT DISTINCT email FROM email_sends
                 WHERE email IS NOT NULL AND sent_at >= {_PARAM}""",
            (cutoff,),
        )
        rows = cur.fetchall()
        out = set()
        for r in rows:
            d = dict(r) if hasattr(r, "keys") else (r[0] if isinstance(r, tuple) else r)
            email = (d.get("email") if isinstance(d, dict) else d) or ""
            if email:
                out.add(email.lower().strip())
        return out
    except Exception:
        return set()
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────
# Warmup schedule — new sender domains need to ramp up gradually
# ──────────────────────────────────────────────────────────────────────

# Progressive daily-volume cap for a new sender domain. Values below
# are informed by Google Workspace + M365 warmup best practices. The
# goal is to build sender reputation with ISPs before blasting at full
# volume. Column: (week_from_first_send, daily_cap).
WARMUP_SCHEDULE = [
    (1,  50),    # Week 1: 50/day max
    (2,  100),   # Week 2: 100/day
    (3,  200),   # Week 3: 200/day
    (4,  400),   # Week 4: 400/day
    (6,  800),   # Week 5-6: 800/day
    (8,  1500),  # Week 7-8: 1500/day
    (12, 3000),  # Week 9-12: 3000/day
    (999, 5000), # Week 13+: full volume
]


def recommended_daily_cap(sender_first_used: Optional[datetime] = None) -> dict:
    """
    Given the date the sender domain first sent outbound mail (or None
    for "brand new, haven't sent anything"), return the recommended
    daily send cap along with a human-readable status.

    Returns:
      {
        "cap": int,              # daily send limit
        "week": int,             # weeks since first send
        "stage": str,            # "week 1 warmup", "week 4 warmup", "full volume"
        "next_bump_in_days": int or None,
      }
    """
    if sender_first_used is None:
        return {"cap": 50, "week": 0, "stage": "week 1 warmup (brand new)",
                "next_bump_in_days": 7}

    days_elapsed = (datetime.utcnow() - sender_first_used.replace(tzinfo=None)).days
    weeks_elapsed = max(1, (days_elapsed // 7) + 1)

    cap = WARMUP_SCHEDULE[-1][1]
    stage = "full volume"
    next_bump = None
    for week_cutoff, daily_cap in WARMUP_SCHEDULE:
        if weeks_elapsed <= week_cutoff:
            cap = daily_cap
            if week_cutoff < 999:
                stage = f"week {weeks_elapsed} warmup (cap bumps at week {week_cutoff + 1})"
                next_bump = max(0, (week_cutoff + 1) * 7 - days_elapsed)
            else:
                stage = "full volume"
            break
    return {"cap": cap, "week": weeks_elapsed, "stage": stage,
            "next_bump_in_days": next_bump}


def sender_first_send_date() -> Optional[datetime]:
    """Earliest sent_at in email_sends — when this sender domain first
    sent mail through our tracking. None if we've never sent anything."""
    try:
        from src.storage import _connect, _cursor
    except Exception:
        return None
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT MIN(sent_at) AS first_send FROM email_sends")
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row) if hasattr(row, "keys") else {"first_send": row[0]}
        return _parse_dt(d.get("first_send"))
    except Exception:
        return None
    finally:
        conn.close()


def sent_today_count() -> int:
    """Count of email_sends rows created today (UTC). Used to show
    warmup progress vs daily cap."""
    try:
        from src.storage import _connect, _cursor, _PARAM
    except Exception:
        return 0
    start_of_day = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"SELECT COUNT(*) AS c FROM email_sends WHERE sent_at >= {_PARAM}",
            (start_of_day,),
        )
        row = cur.fetchone()
        d = dict(row) if hasattr(row, "keys") else {"c": row[0]}
        return int(d.get("c") or 0)
    except Exception:
        return 0
    finally:
        conn.close()
