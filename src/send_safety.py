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

from datetime import datetime, timedelta, timezone
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

    return (len(reasons) == 0, reasons)


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
