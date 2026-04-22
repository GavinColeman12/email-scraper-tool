"""
Gmail → email_sends sync.

Problem: the dashboard reports sends + bounces from the `email_sends`
table. Outreach is happening through Gmail but nothing was populating
the table, so the dashboard showed 0 (or just the seed test rows).

This module scans Gmail for outreach sends and bounce notifications
and writes both to `email_sends`. Run from the dashboard button
'Sync from Gmail' or from a cron.

Design:
  1. Pull threads from `from:me` in the selected window.
  2. Skip threads that look like personal / non-outreach traffic via
     a subject-pattern filter (configurable).
  3. For each outreach thread, upsert a row into email_sends using
     (email, sent_at) as the dedupe key.
  4. Walk mailer-daemon bounce threads; look up the bounced recipient
     in email_sends and mark status='bounced' + bounce_type from the
     SMTP response text.

Cost: free (uses the existing Gmail MCP, no paid APIs).
"""
from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from typing import Iterable, Optional

from src.storage import _connect, _cursor, _PARAM, USE_PG


# ── Subject-pattern filter — identifies outreach threads ──
# You own the outbound copy so this is a fingerprint of YOUR templates.
# Kept broad (OR'd substrings) so we don't miss a future template variant.
OUTREACH_SUBJECT_PATTERNS = (
    "a quick analysis i put together on",
    "independent consultant",
    "here's why",
    "here's what they're doing differently",
    "review volume",
    "competitive analysis",
    "reputation audit",
    "at 4.",   # "at 4.9★" / "at 4.8" style comparisons
)


BOUNCE_SUBJECT_PATTERNS = (
    "delivery status notification",
    "mail delivery",
    "undelivered",
    "address not found",
    "message not delivered",
    "failure notice",
    "mail delivery failed",
)

HARD_BOUNCE_PHRASES = (
    "address not found",
    "user unknown",
    "550 5.1.1",
    "550 5.1.10",
    "550 5.4.1",
    "recipient not found",
    "no such user",
)


def _is_outreach_subject(subject: str) -> bool:
    s = (subject or "").lower()
    return any(p in s for p in OUTREACH_SUBJECT_PATTERNS)


def _is_bounce_subject(subject: str) -> bool:
    s = (subject or "").lower()
    return any(p in s for p in BOUNCE_SUBJECT_PATTERNS)


def _extract_bounced_recipient(snippet: str) -> Optional[str]:
    """Parse the bounced-to address from a mailer-daemon snippet.

    Example snippets:
      'Address not found Your message wasn't delivered to
       marco@midwestconstructionexperts.com because the address...'
    """
    if not snippet:
        return None
    text = html.unescape(snippet)
    m = re.search(
        r"delivered to\s+([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
        text, re.I,
    )
    if m:
        return m.group(1).lower()
    m = re.search(
        r"\b([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})\b",
        text,
    )
    return m.group(1).lower() if m else None


def _classify_bounce_type(snippet: str) -> str:
    s = (snippet or "").lower()
    if any(p in s for p in HARD_BOUNCE_PHRASES):
        return "hard"
    if "temporary" in s or "delay" in s or "retry" in s:
        return "soft"
    return "hard"  # default to hard if we can't tell — safer for reputation


# ── DB upserts ──

def _domain_of(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.split("@", 1)[1].lower()


def _upsert_send(
    *, email: str, business_name: str, business_type: str,
    sent_at: datetime, subject: str,
) -> bool:
    """Insert if (email, sent_at-minute) is new. Returns True if a new row was written."""
    conn = _connect()
    try:
        cur = _cursor(conn)
        # Dedupe on (email, sent_at truncated to minute)
        sent_minute = sent_at.replace(second=0, microsecond=0)
        if USE_PG:
            cur.execute(
                "SELECT id FROM email_sends WHERE email=%s AND sent_at >= %s AND sent_at < %s",
                (email, sent_minute, sent_minute.replace(second=59)),
            )
        else:
            cur.execute(
                "SELECT id FROM email_sends WHERE email=? AND sent_at >= ? AND sent_at < ?",
                (email, sent_minute.isoformat(), sent_minute.replace(second=59).isoformat()),
            )
        if cur.fetchone():
            return False

        pattern_used = _infer_pattern(email)
        domain = _domain_of(email)
        params = (
            email, domain, business_name, (business_type or "").lower(),
            pattern_used, 80, "gmail_sync", sent_at, "sent",
        )
        if USE_PG:
            cur.execute("""
                INSERT INTO email_sends (email, domain, business_name, business_type,
                                          pattern_used, confidence, source, sent_at, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, params)
        else:
            cur.execute("""
                INSERT INTO email_sends (email, domain, business_name, business_type,
                                          pattern_used, confidence, source, sent_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (*params[:7], params[7].isoformat(), params[8]))
        conn.commit()
        return True
    finally:
        conn.close()


def _mark_bounce(email: str, bounce_type: str, bounce_reason: str,
                 received_at: datetime) -> bool:
    """Flip status → bounced on the matching (unbounced) email row.
    Returns True if we found + updated a row."""
    conn = _connect()
    try:
        cur = _cursor(conn)
        if USE_PG:
            cur.execute("""
                UPDATE email_sends
                   SET status='bounced', bounce_type=%s, bounce_reason=%s,
                       bounce_received_at=%s
                 WHERE email=%s AND status='sent'
                   AND sent_at <= %s
                 RETURNING id
            """, (bounce_type, bounce_reason[:500], received_at, email, received_at))
        else:
            cur.execute("""
                UPDATE email_sends
                   SET status='bounced', bounce_type=?, bounce_reason=?,
                       bounce_received_at=?
                 WHERE email=? AND status='sent' AND sent_at <= ?
            """, (bounce_type, bounce_reason[:500], received_at.isoformat(),
                  email, received_at.isoformat()))
        updated = cur.rowcount
        conn.commit()
        return updated > 0
    finally:
        conn.close()


def _infer_pattern(email: str) -> str:
    if not email or "@" not in email:
        return "unknown"
    local = email.split("@", 1)[0]
    if "." in local:
        return "first.last"
    if len(local) <= 2:
        return "short"
    # Can't reliably distinguish flast from first without a DM name;
    # leave as "heuristic" and trust the volume-mode adapter's actual label.
    return "heuristic"


# ── Business-name / business-type lookup for enrichment ──

def _lookup_biz(email: str) -> dict:
    """Find business_name + business_type by matching the email to a
    row in the businesses table (primary_email match, or domain match
    as fallback). Returns {} if no match."""
    domain = _domain_of(email)
    if not domain:
        return {}
    conn = _connect()
    try:
        cur = _cursor(conn)
        # Exact email match first
        cur.execute(
            f"SELECT business_name, business_type FROM businesses "
            f"WHERE LOWER(primary_email) = {_PARAM} LIMIT 1",
            (email.lower(),),
        )
        row = cur.fetchone()
        if row:
            return dict(row)
        # Domain-only fallback
        cur.execute(
            f"SELECT business_name, business_type FROM businesses "
            f"WHERE LOWER(website) LIKE {_PARAM} "
            f"   OR LOWER(primary_email) LIKE {_PARAM} LIMIT 1",
            (f"%{domain}%", f"%@{domain}"),
        )
        row = cur.fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


# ── The sync itself ──

def sync_from_gmail(search_threads_fn, *, days: int = 30) -> dict:
    """
    Main entry. Takes a `search_threads_fn(query, pageSize, pageToken)
    -> dict` callback — the Streamlit page injects the MCP Gmail client
    since it's not importable at module level.

    Returns a summary dict: {'sent': N, 'bounces': N, 'skipped': N}.
    """
    summary = {"sent": 0, "bounces": 0, "skipped": 0, "errors": 0}

    # ── Pass 1: outreach sends ──
    page_token = None
    pages = 0
    while pages < 20:  # safety cap: 20 pages * 50 = 1000 threads
        try:
            result = search_threads_fn(
                query=f"from:me newer_than:{days}d",
                pageSize=50,
                pageToken=page_token,
            )
        except Exception:
            summary["errors"] += 1
            break
        threads = (result or {}).get("threads") or []
        for t in threads:
            for msg in (t.get("messages") or []):
                sender = (msg.get("sender") or "").lower()
                if "@" not in sender or sender.startswith("mailer-daemon"):
                    continue
                subject = msg.get("subject") or ""
                if not _is_outreach_subject(subject):
                    summary["skipped"] += 1
                    continue
                to_addrs = msg.get("toRecipients") or []
                if not to_addrs:
                    continue
                recipient = to_addrs[0].lower().strip()
                try:
                    sent_at = datetime.fromisoformat(
                        msg["date"].replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                except Exception:
                    sent_at = datetime.utcnow()
                biz = _lookup_biz(recipient)
                try:
                    if _upsert_send(
                        email=recipient,
                        business_name=biz.get("business_name") or "",
                        business_type=biz.get("business_type") or "",
                        sent_at=sent_at,
                        subject=subject,
                    ):
                        summary["sent"] += 1
                except Exception:
                    summary["errors"] += 1
        page_token = (result or {}).get("nextPageToken")
        pages += 1
        if not page_token:
            break

    # ── Pass 2: bounces from mailer-daemon ──
    page_token = None
    pages = 0
    while pages < 10:
        try:
            result = search_threads_fn(
                query=(
                    f'(subject:"Delivery Status Notification" OR '
                    f'subject:"Undelivered" OR subject:"Mail Delivery" OR '
                    f'from:mailer-daemon) newer_than:{days}d'
                ),
                pageSize=50,
                pageToken=page_token,
            )
        except Exception:
            summary["errors"] += 1
            break
        threads = (result or {}).get("threads") or []
        for t in threads:
            for msg in (t.get("messages") or []):
                sender = (msg.get("sender") or "").lower()
                if "mailer-daemon" not in sender and "postmaster" not in sender:
                    continue
                subject = msg.get("subject") or ""
                if not _is_bounce_subject(subject):
                    continue
                snippet = msg.get("snippet") or ""
                recipient = _extract_bounced_recipient(snippet)
                if not recipient:
                    continue
                btype = _classify_bounce_type(snippet)
                reason = html.unescape(snippet)[:300]
                try:
                    recv_at = datetime.fromisoformat(
                        msg["date"].replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                except Exception:
                    recv_at = datetime.utcnow()
                try:
                    if _mark_bounce(recipient, btype, reason, recv_at):
                        summary["bounces"] += 1
                except Exception:
                    summary["errors"] += 1
        page_token = (result or {}).get("nextPageToken")
        pages += 1
        if not page_token:
            break

    return summary
