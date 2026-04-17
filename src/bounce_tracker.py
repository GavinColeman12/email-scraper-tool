"""
Bounce tracker — reads Gmail for Mail Delivery Subsystem messages, logs bounces,
and maintains a per-domain/per-pattern success database for learning.

Database schema (3 tables):
- email_sends: one row per outbound email (status: sent/bounced/replied)
- pattern_success: per-domain pattern hit/miss aggregates
- industry_pattern_success: per-industry pattern hit/miss aggregates

Adapts to the existing src.storage backend (Postgres via Neon or SQLite).
"""
import re
import sys
from typing import List, Optional, Tuple

from src.storage import USE_PG, _PARAM, _connect, _cursor, _row_to_dict


_INIT_DONE = False


_SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS email_sends (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    domain TEXT NOT NULL,
    business_name TEXT,
    business_type TEXT,
    pattern_used TEXT,
    confidence INTEGER,
    source TEXT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'sent',
    bounce_reason TEXT,
    bounce_type TEXT,
    bounce_received_at TIMESTAMP,
    reply_received_at TIMESTAMP,
    headcount INTEGER
);

CREATE TABLE IF NOT EXISTS pattern_success (
    domain TEXT NOT NULL,
    pattern TEXT NOT NULL,
    sends INTEGER DEFAULT 0,
    bounces INTEGER DEFAULT 0,
    replies INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (domain, pattern)
);

CREATE TABLE IF NOT EXISTS industry_pattern_success (
    industry TEXT NOT NULL,
    pattern TEXT NOT NULL,
    sends INTEGER DEFAULT 0,
    bounces INTEGER DEFAULT 0,
    replies INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (industry, pattern)
);

CREATE INDEX IF NOT EXISTS idx_email_sends_email ON email_sends(email);
CREATE INDEX IF NOT EXISTS idx_email_sends_status ON email_sends(status);
CREATE INDEX IF NOT EXISTS idx_email_sends_domain ON email_sends(domain);
"""

_SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS email_sends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    domain TEXT NOT NULL,
    business_name TEXT,
    business_type TEXT,
    pattern_used TEXT,
    confidence INTEGER,
    source TEXT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'sent',
    bounce_reason TEXT,
    bounce_type TEXT,
    bounce_received_at TIMESTAMP,
    reply_received_at TIMESTAMP,
    headcount INTEGER
);

CREATE TABLE IF NOT EXISTS pattern_success (
    domain TEXT NOT NULL,
    pattern TEXT NOT NULL,
    sends INTEGER DEFAULT 0,
    bounces INTEGER DEFAULT 0,
    replies INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (domain, pattern)
);

CREATE TABLE IF NOT EXISTS industry_pattern_success (
    industry TEXT NOT NULL,
    pattern TEXT NOT NULL,
    sends INTEGER DEFAULT 0,
    bounces INTEGER DEFAULT 0,
    replies INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (industry, pattern)
);

CREATE INDEX IF NOT EXISTS idx_email_sends_email ON email_sends(email);
CREATE INDEX IF NOT EXISTS idx_email_sends_status ON email_sends(status);
CREATE INDEX IF NOT EXISTS idx_email_sends_domain ON email_sends(domain);
"""


def init_bounce_tables():
    """Create bounce tracking tables. Idempotent."""
    global _INIT_DONE
    if _INIT_DONE:
        return
    conn = _connect()
    try:
        cur = _cursor(conn)
        if USE_PG:
            cur.execute(_SCHEMA_PG)
        else:
            conn.executescript(_SCHEMA_SQLITE)
        conn.commit()
        _INIT_DONE = True
    finally:
        conn.close()


# ── Send + bounce + reply logging ─────────────────────────────────────

def log_send(email, domain, business_name=None, business_type=None,
             pattern_used=None, confidence=None, source=None, headcount=None):
    """Log a sent email. Call from the reputation-audit-tool after Gmail send."""
    init_bounce_tables()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"""INSERT INTO email_sends
                (email, domain, business_name, business_type, pattern_used,
                 confidence, source, headcount)
                VALUES ({_PARAM}, {_PARAM}, {_PARAM}, {_PARAM}, {_PARAM},
                        {_PARAM}, {_PARAM}, {_PARAM})""",
            (email, domain, business_name, business_type, pattern_used,
             confidence, source, headcount),
        )
        if pattern_used and domain:
            _upsert_pattern_stat(cur, "pattern_success",
                                  {"domain": domain, "pattern": pattern_used},
                                  sends_delta=1)
        if pattern_used and business_type:
            _upsert_pattern_stat(cur, "industry_pattern_success",
                                  {"industry": business_type, "pattern": pattern_used},
                                  sends_delta=1)
        conn.commit()
    finally:
        conn.close()


def mark_bounce(email, bounce_reason, bounce_type="hard"):
    """
    Mark an email as bounced. Updates send log + pattern_success DB.
    bounce_type: 'hard' | 'soft' | 'block'
    """
    init_bounce_tables()
    conn = _connect()
    try:
        cur = _cursor(conn)
        # Find most recent send for this email
        cur.execute(
            f"""SELECT id, domain, business_type, pattern_used
                FROM email_sends
                WHERE email = {_PARAM} AND status = 'sent'
                ORDER BY sent_at DESC LIMIT 1""",
            (email,),
        )
        row = cur.fetchone()
        if not row:
            return
        d = _row_to_dict(row)
        send_id = d["id"]
        domain = d.get("domain")
        business_type = d.get("business_type")
        pattern_used = d.get("pattern_used")

        from datetime import datetime
        cur.execute(
            f"""UPDATE email_sends
                SET status = 'bounced',
                    bounce_reason = {_PARAM},
                    bounce_type = {_PARAM},
                    bounce_received_at = {_PARAM}
                WHERE id = {_PARAM}""",
            (bounce_reason, bounce_type, datetime.utcnow().isoformat(), send_id),
        )

        if pattern_used and domain:
            _upsert_pattern_stat(cur, "pattern_success",
                                  {"domain": domain, "pattern": pattern_used},
                                  bounces_delta=1)
        if pattern_used and business_type:
            _upsert_pattern_stat(cur, "industry_pattern_success",
                                  {"industry": business_type, "pattern": pattern_used},
                                  bounces_delta=1)
        conn.commit()
    finally:
        conn.close()


def mark_reply(email):
    """Mark an email as replied. Highest-value signal."""
    init_bounce_tables()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"""SELECT id, domain, business_type, pattern_used
                FROM email_sends
                WHERE email = {_PARAM} AND reply_received_at IS NULL
                ORDER BY sent_at DESC LIMIT 1""",
            (email,),
        )
        row = cur.fetchone()
        if not row:
            return
        d = _row_to_dict(row)
        send_id = d["id"]
        domain = d.get("domain")
        business_type = d.get("business_type")
        pattern_used = d.get("pattern_used")

        from datetime import datetime
        cur.execute(
            f"""UPDATE email_sends
                SET status = 'replied',
                    reply_received_at = {_PARAM}
                WHERE id = {_PARAM}""",
            (datetime.utcnow().isoformat(), send_id),
        )

        if pattern_used and domain:
            _upsert_pattern_stat(cur, "pattern_success",
                                  {"domain": domain, "pattern": pattern_used},
                                  replies_delta=1)
        if pattern_used and business_type:
            _upsert_pattern_stat(cur, "industry_pattern_success",
                                  {"industry": business_type, "pattern": pattern_used},
                                  replies_delta=1)
        conn.commit()
    finally:
        conn.close()


def _upsert_pattern_stat(cursor, table, key, sends_delta=0, bounces_delta=0, replies_delta=0):
    """Upsert into pattern_success or industry_pattern_success table."""
    key_cols = list(key.keys())
    key_vals = list(key.values())

    if USE_PG:
        query = f"""
            INSERT INTO {table} ({', '.join(key_cols)}, sends, bounces, replies, last_updated)
            VALUES ({', '.join(['%s'] * len(key_cols))}, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT ({', '.join(key_cols)}) DO UPDATE SET
                sends = {table}.sends + EXCLUDED.sends,
                bounces = {table}.bounces + EXCLUDED.bounces,
                replies = {table}.replies + EXCLUDED.replies,
                last_updated = CURRENT_TIMESTAMP
        """
        cursor.execute(query, key_vals + [sends_delta, bounces_delta, replies_delta])
    else:
        # SQLite path: try update first, insert if no rows changed
        where = " AND ".join(f"{k} = ?" for k in key_cols)
        cursor.execute(
            f"""UPDATE {table} SET
                sends = sends + ?,
                bounces = bounces + ?,
                replies = replies + ?,
                last_updated = CURRENT_TIMESTAMP
            WHERE {where}""",
            [sends_delta, bounces_delta, replies_delta] + key_vals,
        )
        if cursor.rowcount == 0:
            cursor.execute(
                f"""INSERT INTO {table} ({', '.join(key_cols)}, sends, bounces, replies)
                    VALUES ({', '.join(['?'] * len(key_cols))}, ?, ?, ?)""",
                key_vals + [sends_delta, bounces_delta, replies_delta],
            )


# ── Pattern success queries ───────────────────────────────────────────

def get_domain_pattern_stats(domain):
    """Return pattern success stats for a specific domain."""
    init_bounce_tables()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"""SELECT pattern, sends, bounces, replies
                FROM pattern_success
                WHERE domain = {_PARAM} AND sends > 0
                ORDER BY (CAST(sends - bounces AS FLOAT) / sends) DESC""",
            (domain,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    results = []
    for row in rows:
        d = _row_to_dict(row)
        sends = int(d["sends"] or 0)
        bounces = int(d["bounces"] or 0)
        replies = int(d["replies"] or 0)
        results.append({
            "pattern": d["pattern"],
            "sends": sends,
            "bounces": bounces,
            "replies": replies,
            "delivery_rate": (sends - bounces) / sends if sends > 0 else 0,
            "reply_rate": replies / sends if sends > 0 else 0,
        })
    return results


def get_industry_pattern_stats(industry, min_sends=3):
    """Per-industry pattern success stats, filtered by sample size."""
    init_bounce_tables()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"""SELECT pattern, sends, bounces, replies
                FROM industry_pattern_success
                WHERE industry = {_PARAM} AND sends >= {_PARAM}
                ORDER BY (CAST(sends - bounces AS FLOAT) / sends) DESC""",
            (industry, min_sends),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    results = []
    for row in rows:
        d = _row_to_dict(row)
        sends = int(d["sends"] or 0)
        bounces = int(d["bounces"] or 0)
        replies = int(d["replies"] or 0)
        results.append({
            "pattern": d["pattern"],
            "sends": sends,
            "bounces": bounces,
            "replies": replies,
            "delivery_rate": (sends - bounces) / sends if sends > 0 else 0,
            "reply_rate": replies / sends if sends > 0 else 0,
        })
    return results


# ── Gmail bounce parsing ──────────────────────────────────────────────

def parse_gmail_bounces(gmail_service, lookback_days=7):
    """
    Parse Gmail inbox for Mail Delivery Subsystem bounce messages.
    Updates email_sends table with bounce info.
    Returns count of bounces processed.
    """
    query = f'from:mailer-daemon OR from:"mail delivery subsystem" newer_than:{lookback_days}d'

    try:
        result = gmail_service.users().messages().list(userId='me', q=query).execute()
        messages = result.get('messages', [])
    except Exception as e:
        print(f"[bounce_tracker] Gmail fetch error: {e}", file=sys.stderr)
        return 0

    processed = 0
    for msg_meta in messages:
        try:
            msg = gmail_service.users().messages().get(
                userId='me', id=msg_meta['id']
            ).execute()
            payload = msg.get('payload', {})
            body = _extract_body(payload)
            bounced_email, reason = _parse_bounce_body(body)
            if bounced_email:
                bounce_type = _classify_bounce(reason)
                mark_bounce(bounced_email, reason, bounce_type)
                processed += 1
        except Exception as e:
            print(f"[bounce_tracker] Error parsing message: {e}", file=sys.stderr)
            continue
    return processed


def _extract_body(payload):
    """Extract text body from Gmail API payload."""
    import base64

    body_data = payload.get('body', {}).get('data', '')
    if body_data:
        return base64.urlsafe_b64decode(body_data + '==').decode('utf-8', errors='ignore')

    for part in payload.get('parts', []):
        if part.get('mimeType') == 'text/plain':
            data = part.get('body', {}).get('data', '')
            if data:
                return base64.urlsafe_b64decode(data + '==').decode('utf-8', errors='ignore')
        if 'parts' in part:
            result = _extract_body(part)
            if result:
                return result
    return ''


def _parse_bounce_body(body):
    """Extract bounced email address and reason from Mail Delivery Subsystem body."""
    if not body:
        return (None, "Unknown bounce")

    email_patterns = [
        r"Your message wasn't delivered to (\S+@\S+)",
        r"delivery to the following recipient failed.*?(\S+@\S+)",
        r"<(\S+@\S+)>:\s*host",
        r"failed recipient:\s*(\S+@\S+)",
        r"RCPT TO:<(\S+@\S+)>",
    ]

    bounced_email = None
    for pattern in email_patterns:
        match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
        if match:
            bounced_email = match.group(1).strip('<>,;. ')
            break

    reason_patterns = [
        r"(550[- ].*?)(?:\n\n|\r\n\r\n)",
        r"(The email account .*?doesn't exist)",
        r"(Address not found)",
        r"(User unknown)",
        r"(Mailbox unavailable)",
    ]

    reason = "Unknown bounce"
    for pattern in reason_patterns:
        match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
        if match:
            reason = match.group(1).strip()[:200]
            break

    return (bounced_email, reason)


def _classify_bounce(reason):
    """Classify bounce as hard/soft/block based on reason text."""
    reason_lower = (reason or "").lower()
    hard_indicators = ["550", "does not exist", "user unknown", "no such user",
                        "address not found", "invalid recipient"]
    soft_indicators = ["mailbox full", "over quota", "temporarily unavailable",
                        "try again later"]
    block_indicators = ["spam", "blocked", "denied", "blacklist", "policy reject"]

    if any(ind in reason_lower for ind in hard_indicators):
        return "hard"
    if any(ind in reason_lower for ind in soft_indicators):
        return "soft"
    if any(ind in reason_lower for ind in block_indicators):
        return "block"
    return "hard"
