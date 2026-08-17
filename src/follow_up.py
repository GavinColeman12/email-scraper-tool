"""
Second-touch follow-up exporter.

Cold-outreach playbook: first email sent, ~7 days later send a
second touch. This module identifies the prospects eligible for that
second send by joining email_sends history with the businesses table,
filtering out:

  - bounces (don't re-send to a bounced address)
  - replies (already engaged — they'd reply in-thread, not need a
    second touch)
  - too-recent sends (sent < `days_min` days ago — give them time)
  - too-old sends (sent > `days_max` days ago — stale prospects)
  - already-followed-up (≥2 sends to same email)
  - domains in the bounce blocklist (any prior bounce on the domain
    poisons future sends there)

Returns a list of dicts ready for CSV export with all the fields
needed for outreach personalization (name, business, phone, address,
website, original send date, days since first touch).

Cost: $0 — pure DB query, no API calls.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta


def find_followup_candidates(
    *,
    days_min: int = 7,
    days_max: int = 30,
    exclude_bounce_domains: bool = True,
    exclude_generic_locals: bool = True,
) -> list[dict]:
    """
    Return prospects eligible for a second-touch send.

    Args:
      days_min: minimum days since first send (default 7)
      days_max: maximum days since first send (default 30 — older
        prospects are stale; consider a fresh first-touch instead)
      exclude_bounce_domains: when True, drop any email whose domain
        has had a hard bounce on any other send. Default True for
        sender-reputation safety.
      exclude_generic_locals: when True, drop emails whose local part
        is a shared inbox (info@, contact@, sales@, manager@, etc.).
        First-touch sends to these often happened pre-stopword-list
        and didn't reach a real person — following up there mostly
        annoys reception staff. Default True.

    Returns:
      List of dicts with full row data for CSV export, sorted by
      sent_at ascending (oldest-first — these are the most overdue
      for a follow-up). Each row is the FIRST send for that email
      (so the user knows when they originally reached out).
    """
    try:
        from src.storage import _connect, _cursor, _PARAM
    except Exception:
        return []

    cutoff_old = datetime.utcnow() - timedelta(days=days_min)
    cutoff_stale = datetime.utcnow() - timedelta(days=days_max)

    conn = _connect()
    try:
        cur = _cursor(conn)
        # Pull ALL sends in the window — we'll group by email in
        # Python so we can detect duplicate sends (already-followed-up)
        # and pick the earliest as the first-touch reference.
        cur.execute(f"""
            SELECT s.id, s.email, s.domain, s.business_name, s.business_type,
                   s.sent_at, s.status, s.bounce_received_at,
                   s.reply_received_at, s.confidence, s.source,
                   b.address, b.phone, b.website,
                   b.contact_name, b.contact_title, b.rating, b.review_count
              FROM email_sends s
              LEFT JOIN businesses b ON LOWER(b.business_name) =
                                          LOWER(s.business_name)
             WHERE s.email IS NOT NULL AND s.email <> ''
               AND s.sent_at >= {_PARAM}
               AND s.sent_at <= {_PARAM}
        """, (cutoff_stale, cutoff_old))
        rows = [dict(r) for r in cur.fetchall()]

        # Build the bounce-domain blocklist if requested
        bounce_domains: set = set()
        if exclude_bounce_domains:
            cur.execute("""
                SELECT DISTINCT domain FROM email_sends
                 WHERE status = 'bounced' AND domain IS NOT NULL
                   AND domain <> ''
            """)
            for r in cur.fetchall():
                d = (dict(r).get("domain") or "").lower().strip()
                if d:
                    bounce_domains.add(d)

        # Get FULL send-history per email for dedupe + already-replied
        # check (must look at ALL sends to that email, not just the
        # ones in our window)
        cur.execute("""
            SELECT email, status, reply_received_at, bounce_received_at
              FROM email_sends
             WHERE email IS NOT NULL AND email <> ''
        """)
        history_by_email: dict = defaultdict(list)
        for r in cur.fetchall():
            d = dict(r)
            email = (d.get("email") or "").lower().strip()
            if email:
                history_by_email[email].append(d)
    finally:
        conn.close()

    # Group rows in the window by email; pick the FIRST send as the
    # canonical first-touch
    by_email: dict = defaultdict(list)
    for row in rows:
        email = (row.get("email") or "").lower().strip()
        if email:
            by_email[email].append(row)

    candidates: list[dict] = []
    for email, sends_in_window in by_email.items():
        # First send (oldest) is what we follow up on
        sends_in_window.sort(key=lambda x: x.get("sent_at") or datetime.min)
        first_send = sends_in_window[0]

        # Reject reasons (operate on FULL history, not just window)
        full_history = history_by_email.get(email, [])
        if any(h.get("status") == "bounced" or h.get("bounce_received_at")
                for h in full_history):
            continue  # bounced before — don't re-send
        if any(h.get("reply_received_at") for h in full_history):
            continue  # already replied — they're engaged
        if len(full_history) >= 2:
            continue  # already follow-up sent at least once

        domain = (first_send.get("domain") or "").lower().strip()
        if exclude_bounce_domains and domain in bounce_domains:
            continue  # poisoned domain

        # Generic-local filter — uses the canonical is_generic from
        # the volume_mode stopwords so the same definition the scraper
        # enforces on new picks also gates follow-ups on old ones.
        if exclude_generic_locals:
            try:
                from src.volume_mode.stopwords import email_is_generic
                if email_is_generic(
                    email,
                    business_name=first_send.get("business_name") or "",
                ):
                    continue
            except Exception:
                pass

        sent_at = first_send.get("sent_at")
        if isinstance(sent_at, str):
            try:
                sent_at = datetime.fromisoformat(sent_at)
            except Exception:
                sent_at = None
        days_ago = (datetime.utcnow() - sent_at).days if sent_at else 0

        candidates.append({
            "email": email,
            "business_name": first_send.get("business_name") or "",
            "business_type": first_send.get("business_type") or "",
            "domain": domain,
            "contact_name": first_send.get("contact_name") or "",
            "contact_title": first_send.get("contact_title") or "",
            "address": first_send.get("address") or "",
            "phone": first_send.get("phone") or "",
            "website": first_send.get("website") or "",
            "rating": first_send.get("rating") or 0,
            "review_count": first_send.get("review_count") or 0,
            "first_sent_at": sent_at.isoformat() if sent_at else "",
            "days_since_first_touch": days_ago,
            "first_touch_pattern": first_send.get("source") or "",
            "first_touch_confidence": first_send.get("confidence") or 0,
        })

    # Oldest first — most overdue follow-ups at the top
    candidates.sort(key=lambda x: x.get("days_since_first_touch") or 0,
                     reverse=True)
    return candidates


def followup_summary() -> dict:
    """Quick stats for the UI: count of eligible follow-ups + the
    breakdown by reason for excluded rows. Used to show the operator
    what they're filtering against."""
    try:
        from src.storage import _connect, _cursor
    except Exception:
        return {}
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT COUNT(*) AS c FROM email_sends")
        total_sends = int(dict(cur.fetchone()).get("c") or 0)
        cur.execute(
            "SELECT COUNT(*) AS c FROM email_sends WHERE status='bounced'"
        )
        total_bounced = int(dict(cur.fetchone()).get("c") or 0)
        cur.execute(
            "SELECT COUNT(*) AS c FROM email_sends "
            "WHERE reply_received_at IS NOT NULL"
        )
        total_replied = int(dict(cur.fetchone()).get("c") or 0)
        # Distinct emails sent
        cur.execute(
            "SELECT COUNT(DISTINCT email) AS c FROM email_sends "
            "WHERE email IS NOT NULL AND email <> ''"
        )
        distinct_emails = int(dict(cur.fetchone()).get("c") or 0)
    finally:
        conn.close()
    return {
        "total_sends": total_sends,
        "total_bounced": total_bounced,
        "total_replied": total_replied,
        "distinct_emails": distinct_emails,
    }
