"""
Dashboard queries — aggregate stats for the home-page dashboard.

All queries tolerate missing rows (empty DB) and the Postgres/SQLite
duality via storage._connect / _cursor.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Optional

from src.storage import _connect, _cursor, _PARAM, USE_PG, init_db


# ────────────────────────────────────────────────────────────────────────
# Lifetime KPIs
# ────────────────────────────────────────────────────────────────────────

def lifetime_kpis() -> dict:
    """
    Single-row snapshot of everything we know across every search.
    Used for the top metric cards.
    """
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        # Search + business counts — single SQL for cheap cross-join
        cur.execute("""
            SELECT
              (SELECT COUNT(*) FROM searches) AS search_count,
              (SELECT COUNT(*) FROM businesses) AS biz_count,
              (SELECT COUNT(*) FROM businesses
                 WHERE primary_email IS NOT NULL AND primary_email <> '') AS emails_found,
              (SELECT COUNT(*) FROM businesses
                 WHERE contact_name IS NOT NULL AND contact_name <> ''
                   AND contact_title IS NOT NULL AND contact_title <> '') AS dms_identified,
              (SELECT COUNT(*) FROM businesses
                 WHERE email_safe_to_send = 1) AS safe_to_send,
              (SELECT COUNT(*) FROM email_sends) AS total_sent,
              (SELECT COUNT(*) FROM email_sends WHERE status = 'bounced') AS total_bounced,
              (SELECT COUNT(*) FROM email_sends WHERE status = 'bounced'
                   AND bounce_type = 'hard') AS hard_bounces,
              (SELECT COUNT(*) FROM email_sends
                 WHERE reply_received_at IS NOT NULL) AS replies
        """)
        row = cur.fetchone() or {}
        d = dict(row)
        # Rates
        sent = d.get("total_sent") or 0
        d["bounce_rate"] = round(
            100.0 * (d.get("total_bounced") or 0) / sent, 1
        ) if sent else 0.0
        d["reply_rate"] = round(
            100.0 * (d.get("replies") or 0) / sent, 1
        ) if sent else 0.0
        # DM-plus-email combined metric — the lead that's actually
        # actionable (real person identified + deliverable address)
        cur.execute("""
            SELECT COUNT(*) AS n FROM businesses
            WHERE primary_email IS NOT NULL AND primary_email <> ''
              AND contact_name IS NOT NULL AND contact_name <> ''
              AND email_safe_to_send = 1
        """)
        d["actionable_leads"] = (cur.fetchone() or {}).get("n", 0) or 0
        return d
    finally:
        conn.close()


# ────────────────────────────────────────────────────────────────────────
# Daily trend (for the chart)
# ────────────────────────────────────────────────────────────────────────

def daily_rollup(days: int = 30, industry: Optional[str] = None) -> list[dict]:
    """
    Day-by-day counts of [emails_found, emails_sent, bounces] over the
    last N days. `industry` filters by business_type substring (case-
    insensitive) when provided.

    Returns a zero-filled series — missing days render as 0 in the chart
    instead of dropping out.
    """
    init_db()
    cutoff = (datetime.utcnow() - timedelta(days=days)).date()
    industry_like = f"%{industry.lower()}%" if industry else None

    conn = _connect()
    try:
        cur = _cursor(conn)

        # Emails found — indexed by the day the scraper wrote them.
        # Prefer scraped_at; fall back to created_at for businesses
        # that never got a scrape timestamp.
        if USE_PG:
            biz_sql = """
                SELECT DATE(COALESCE(scraped_at, created_at)) AS day,
                       COUNT(*) FILTER (WHERE primary_email IS NOT NULL AND primary_email <> '') AS found,
                       COUNT(*) FILTER (WHERE contact_name IS NOT NULL AND contact_name <> ''
                                        AND contact_title IS NOT NULL AND contact_title <> '') AS dms,
                       COUNT(*) FILTER (WHERE email_safe_to_send = 1) AS safe
                FROM businesses
                WHERE COALESCE(scraped_at, created_at) >= %s
                  AND (%s::text IS NULL OR LOWER(COALESCE(business_type,'')) LIKE %s)
                GROUP BY DATE(COALESCE(scraped_at, created_at))
                ORDER BY day
            """
            cur.execute(biz_sql, (cutoff, industry, industry_like))
        else:
            biz_sql = """
                SELECT DATE(COALESCE(scraped_at, created_at)) AS day,
                       SUM(CASE WHEN primary_email IS NOT NULL AND primary_email <> '' THEN 1 ELSE 0 END) AS found,
                       SUM(CASE WHEN contact_name IS NOT NULL AND contact_name <> ''
                                AND contact_title IS NOT NULL AND contact_title <> '' THEN 1 ELSE 0 END) AS dms,
                       SUM(CASE WHEN email_safe_to_send = 1 THEN 1 ELSE 0 END) AS safe
                FROM businesses
                WHERE DATE(COALESCE(scraped_at, created_at)) >= ?
                  AND (? IS NULL OR LOWER(COALESCE(business_type,'')) LIKE ?)
                GROUP BY DATE(COALESCE(scraped_at, created_at))
                ORDER BY day
            """
            cur.execute(biz_sql, (cutoff.isoformat(), industry, industry_like))
        biz_rows = {str(r["day"]): dict(r) for r in cur.fetchall()}

        # Email sends — keyed by sent_at day. Industry filter joins
        # against email_sends.business_type.
        if USE_PG:
            sends_sql = """
                SELECT DATE(sent_at) AS day,
                       COUNT(*) AS sent,
                       COUNT(*) FILTER (WHERE status = 'bounced') AS bounces,
                       COUNT(*) FILTER (WHERE reply_received_at IS NOT NULL) AS replies
                FROM email_sends
                WHERE sent_at >= %s
                  AND (%s::text IS NULL OR LOWER(COALESCE(business_type,'')) LIKE %s)
                GROUP BY DATE(sent_at)
                ORDER BY day
            """
            cur.execute(sends_sql, (cutoff, industry, industry_like))
        else:
            sends_sql = """
                SELECT DATE(sent_at) AS day,
                       COUNT(*) AS sent,
                       SUM(CASE WHEN status = 'bounced' THEN 1 ELSE 0 END) AS bounces,
                       SUM(CASE WHEN reply_received_at IS NOT NULL THEN 1 ELSE 0 END) AS replies
                FROM email_sends
                WHERE DATE(sent_at) >= ?
                  AND (? IS NULL OR LOWER(COALESCE(business_type,'')) LIKE ?)
                GROUP BY DATE(sent_at)
                ORDER BY day
            """
            cur.execute(sends_sql, (cutoff.isoformat(), industry, industry_like))
        send_rows = {str(r["day"]): dict(r) for r in cur.fetchall()}

        # Zero-fill the full date range so the chart doesn't drop days
        out = []
        for i in range(days + 1):
            day = cutoff + timedelta(days=i)
            key = str(day)
            biz = biz_rows.get(key, {})
            snd = send_rows.get(key, {})
            out.append({
                "day": key,
                "found": int(biz.get("found") or 0),
                "dms": int(biz.get("dms") or 0),
                "safe": int(biz.get("safe") or 0),
                "sent": int(snd.get("sent") or 0),
                "bounces": int(snd.get("bounces") or 0),
                "replies": int(snd.get("replies") or 0),
            })
        return out
    finally:
        conn.close()


# ────────────────────────────────────────────────────────────────────────
# Enriched per-search stats (for the table below the charts)
# ────────────────────────────────────────────────────────────────────────

def enriched_searches(limit: int = 50) -> list[dict]:
    """
    Recent searches enriched with scrape + send + bounce counters.

    Join strategy: email_sends → business_name match (not all rows link
    cleanly, but it's the best signal we have without a FK). Where
    business_name isn't unique across searches, counts may over-report
    slightly — acceptable for a dashboard summary.
    """
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        # Pull all searches + aggregated biz stats
        cur.execute("""
            SELECT s.id, s.query, s.location, s.max_results, s.created_at,
                   COUNT(b.id) AS biz_count,
                   SUM(CASE WHEN b.scraped_at IS NOT NULL THEN 1 ELSE 0 END) AS scraped,
                   SUM(CASE WHEN b.primary_email IS NOT NULL AND b.primary_email <> ''
                            THEN 1 ELSE 0 END) AS emails_found,
                   SUM(CASE WHEN b.contact_name IS NOT NULL AND b.contact_name <> ''
                            AND b.contact_title IS NOT NULL AND b.contact_title <> ''
                            THEN 1 ELSE 0 END) AS dms_identified,
                   SUM(CASE WHEN b.email_safe_to_send = 1 THEN 1 ELSE 0 END) AS safe_to_send
            FROM searches s
            LEFT JOIN businesses b ON b.search_id = s.id
            GROUP BY s.id, s.query, s.location, s.max_results, s.created_at
            ORDER BY s.created_at DESC
            LIMIT {lim}
        """.replace("{lim}", str(int(limit))))
        rows = [dict(r) for r in cur.fetchall()]

        # Per-search send + bounce counts via business_name join
        for r in rows:
            cur.execute(f"""
                SELECT COUNT(*) AS sent,
                       SUM(CASE WHEN status = 'bounced' THEN 1 ELSE 0 END) AS bounced,
                       SUM(CASE WHEN reply_received_at IS NOT NULL THEN 1 ELSE 0 END) AS replies
                FROM email_sends
                WHERE business_name IN (
                    SELECT business_name FROM businesses WHERE search_id = {_PARAM}
                )
            """, (r["id"],))
            agg = cur.fetchone() or {}
            r["sent"] = int(agg.get("sent") or 0)
            r["bounced"] = int(agg.get("bounced") or 0)
            r["replies"] = int(agg.get("replies") or 0)
            r["bounce_rate"] = (
                round(100.0 * r["bounced"] / r["sent"], 1) if r["sent"] else 0.0
            )
        return rows
    finally:
        conn.close()


# ────────────────────────────────────────────────────────────────────────
# Industry options for the filter dropdown
# ────────────────────────────────────────────────────────────────────────

def industry_options() -> list[str]:
    """
    Distinct business_type values that actually exist in the DB, sorted
    alphabetically. Used to populate the industry filter dropdown.
    """
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute("""
            SELECT DISTINCT LOWER(business_type) AS bt
            FROM businesses
            WHERE business_type IS NOT NULL AND business_type <> ''
            ORDER BY bt
        """)
        return [r["bt"] for r in cur.fetchall() if r["bt"]]
    finally:
        conn.close()
