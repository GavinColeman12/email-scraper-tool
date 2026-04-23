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
    last N days. `industry` filters by macro vertical (see
    normalize_vertical) when provided — e.g. 'Law / Legal' aggregates
    'attorney', 'law firm', 'bankruptcy attorney', etc.

    Returns a zero-filled series — missing days render as 0 in the chart
    instead of dropping out.
    """
    init_db()
    cutoff = (datetime.utcnow() - timedelta(days=days)).date()

    conn = _connect()
    try:
        cur = _cursor(conn)

        # Pull raw rows (no SQL-level industry filter — we aggregate by
        # macro vertical in Python via normalize_vertical). Cheap even
        # at 10k+ businesses because it's GROUP-BY-day+business_type.
        if USE_PG:
            cur.execute("""
                SELECT DATE(COALESCE(scraped_at, created_at)) AS day,
                       business_type,
                       COUNT(*) FILTER (WHERE primary_email IS NOT NULL AND primary_email <> '') AS found,
                       COUNT(*) FILTER (WHERE contact_name IS NOT NULL AND contact_name <> ''
                                        AND contact_title IS NOT NULL AND contact_title <> '') AS dms,
                       COUNT(*) FILTER (WHERE email_safe_to_send = 1) AS safe
                FROM businesses
                WHERE COALESCE(scraped_at, created_at) >= %s
                GROUP BY day, business_type
            """, (cutoff,))
        else:
            cur.execute("""
                SELECT DATE(COALESCE(scraped_at, created_at)) AS day,
                       business_type,
                       SUM(CASE WHEN primary_email IS NOT NULL AND primary_email <> '' THEN 1 ELSE 0 END) AS found,
                       SUM(CASE WHEN contact_name IS NOT NULL AND contact_name <> ''
                                AND contact_title IS NOT NULL AND contact_title <> '' THEN 1 ELSE 0 END) AS dms,
                       SUM(CASE WHEN email_safe_to_send = 1 THEN 1 ELSE 0 END) AS safe
                FROM businesses
                WHERE DATE(COALESCE(scraped_at, created_at)) >= ?
                GROUP BY day, business_type
            """, (cutoff.isoformat(),))
        biz_rows: dict[str, dict] = {}
        for r in cur.fetchall():
            # Python-side vertical filter (keeps the SQL dialect-neutral)
            if industry and normalize_vertical(r["business_type"] or "") != industry:
                continue
            key = str(r["day"])
            agg = biz_rows.setdefault(key, {"found": 0, "dms": 0, "safe": 0})
            agg["found"] += int(r["found"] or 0)
            agg["dms"] += int(r["dms"] or 0)
            agg["safe"] += int(r["safe"] or 0)

        if USE_PG:
            cur.execute("""
                SELECT DATE(sent_at) AS day,
                       business_type,
                       COUNT(*) AS sent,
                       COUNT(*) FILTER (WHERE status = 'bounced') AS bounces,
                       COUNT(*) FILTER (WHERE reply_received_at IS NOT NULL) AS replies
                FROM email_sends
                WHERE sent_at >= %s
                GROUP BY day, business_type
            """, (cutoff,))
        else:
            cur.execute("""
                SELECT DATE(sent_at) AS day,
                       business_type,
                       COUNT(*) AS sent,
                       SUM(CASE WHEN status = 'bounced' THEN 1 ELSE 0 END) AS bounces,
                       SUM(CASE WHEN reply_received_at IS NOT NULL THEN 1 ELSE 0 END) AS replies
                FROM email_sends
                WHERE DATE(sent_at) >= ?
                GROUP BY day, business_type
            """, (cutoff.isoformat(),))
        send_rows: dict[str, dict] = {}
        for r in cur.fetchall():
            if industry and normalize_vertical(r["business_type"] or "") != industry:
                continue
            key = str(r["day"])
            agg = send_rows.setdefault(key, {"sent": 0, "bounces": 0, "replies": 0})
            agg["sent"] += int(r["sent"] or 0)
            agg["bounces"] += int(r["bounces"] or 0)
            agg["replies"] += int(r["replies"] or 0)

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
# Industry normalization — raw business_type → macro vertical bucket
# ────────────────────────────────────────────────────────────────────────

# Uses the SAME verticals as volume_mode/priors.py so dashboard filters
# match what the scraper actually treats as a category. Adds a few
# grab-bag verticals the scraper doesn't need but operators want to see
# separated (restaurants, retail, fitness, auto, beauty).
_VERTICAL_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("Law / Legal",        ("law", "attorney", "lawyer", "legal", "litigator",
                              "bankruptcy", "divorce", "injury", "dui")),
    ("Dental",             ("dental", "dentist", "orthodontist", "oral surgeon",
                              "periodontist", "endodontist")),
    ("Medical / Clinic",   ("medical", "physician", "doctor", "clinic", "urgent care",
                              "pediatrician", "chiropractor", "physical therap",
                              "therapy", "family medicine", "cardiology", "dermatology")),
    ("Medspa / Aesthetic", ("med spa", "medspa", "medical spa", "aesthetic", "botox")),
    ("Veterinary",         ("veterinar", "vet clinic", "animal hospital", "pet clinic")),
    ("Accounting / CPA",   ("cpa", "accountant", "accounting", "tax", "bookkeep")),
    ("Financial / RIA",    ("financial advisor", "wealth", "investment advisor",
                              "registered investment")),
    ("Agency / Consulting", ("agency", "consult", "marketing", "advertising",
                              "branding", "design", "pr firm")),
    ("Real Estate / CRE",  ("real estate", "realtor", "property management", "broker")),
    ("Construction / Trades", ("construction", "contractor", "builder", "roofer",
                                "plumb", "hvac", "electric", "remodel", "landscap",
                                "painting", "flooring")),
    ("Manufacturing",      ("manufactur", "industrial", "fabrication", "machine shop")),
    ("Tech / SaaS",        ("software", "saas", "technology", "tech company",
                              "it services", "cloud")),
    ("Nonprofit",          ("nonprofit", "non-profit", "charity", "foundation")),
    ("Education",          ("school", "university", "college", "tutor", "educational")),
    ("Restaurant / Food",  ("restaurant", "cafe", "diner", "bistro", "eatery",
                              "bakery", "bar ", " bar")),
    ("Retail",             ("retail", "store", "shop", "boutique")),
    ("Fitness / Wellness", ("gym", "fitness", "yoga", "pilates", "wellness")),
    ("Beauty / Salon",     ("salon", "barber", "nail", "hair stylist", "spa")),
    ("Auto",               ("auto", "mechanic", "dealership", "car ")),
    ("Home services",      ("cleaning", "pest control", "moving", "handyman")),
]


def normalize_vertical(raw: str) -> str:
    """
    Map a raw Google Maps business_type to one of ~20 macro verticals.
    Returns 'Other' for unmatched — keeps the filter dropdown readable.
    """
    if not raw:
        return "Other"
    t = raw.lower().strip()
    for bucket, patterns in _VERTICAL_PATTERNS:
        for p in patterns:
            if p in t:
                return bucket
    return "Other"


def search_metadata(search_ids: list[int] | None = None) -> dict[int, dict]:
    """
    Per-search summary used by the Replay dropdowns. Fetches biz count,
    primary industry (normalized vertical of the most common business_type),
    and created_at for each search.

    Returns {search_id: {biz_count, primary_industry, created_at, query}}.
    Pass `search_ids=None` to get every search.
    """
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        if search_ids:
            # Parameter list for the IN clause — build placeholders inline.
            ph = ",".join([_PARAM] * len(search_ids))
            cur.execute(
                f"SELECT id, query, created_at FROM searches WHERE id IN ({ph})",
                tuple(search_ids),
            )
        else:
            cur.execute("SELECT id, query, created_at FROM searches")
        searches = [dict(r) for r in cur.fetchall()]

        out: dict[int, dict] = {}
        for s in searches:
            sid = int(s["id"])
            # Biz count + most-common business_type in one pass.
            cur.execute(f"""
                SELECT business_type, COUNT(*) AS c
                FROM businesses
                WHERE search_id = {_PARAM}
                GROUP BY business_type
                ORDER BY c DESC
            """, (sid,))
            rows = cur.fetchall()
            total = sum(int(r["c"] or 0) for r in rows)
            top_raw = rows[0]["business_type"] if rows else ""
            out[sid] = {
                "biz_count": total,
                "primary_industry": normalize_vertical(top_raw or ""),
                "created_at": s.get("created_at"),
                "query": s.get("query") or "",
            }
        return out
    finally:
        conn.close()


def industry_options() -> list[str]:
    """
    Return the macro verticals that actually have data in the DB,
    sorted by count descending (most-used first).
    """
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute("""
            SELECT LOWER(business_type) AS bt, COUNT(*) AS c
            FROM businesses
            WHERE business_type IS NOT NULL AND business_type <> ''
            GROUP BY bt
        """)
        tally: dict[str, int] = {}
        for r in cur.fetchall():
            v = normalize_vertical(r["bt"])
            tally[v] = tally.get(v, 0) + int(r["c"] or 0)
        return [v for v, _ in sorted(tally.items(), key=lambda x: -x[1])]
    finally:
        conn.close()


# ────────────────────────────────────────────────────────────────────────
# Multi-industry rollup — one line per vertical for a trend chart
# ────────────────────────────────────────────────────────────────────────

def outreach_by_location(days: int = 365) -> dict:
    """
    Where our outreach is actually going. Joins email_sends →
    businesses.address (via business_name match, which is our
    best-available FK-lite join), parses 'City, ST ZIP' out of
    the freeform address string, and counts bysend count.

    Returns:
      {
        'by_state': [{'state': 'TX', 'sent': N, 'bounced': N}, ...],
        'by_city':  [{'city': 'Austin, TX', 'sent': N, 'bounced': N}, ...],
      }
    Sorted by sent DESC, top 15 each.
    """
    init_db()
    cutoff = (datetime.utcnow() - timedelta(days=days)).date()
    conn = _connect()
    try:
        cur = _cursor(conn)
        # Pull sends + the matched business address. LEFT JOIN via
        # business_name — business_name isn't unique, but it's the
        # strongest signal available without a migration to add an FK.
        if USE_PG:
            cur.execute("""
                SELECT s.status, s.sent_at,
                       COALESCE(b.address, b.location, '') AS addr
                FROM email_sends s
                LEFT JOIN businesses b ON b.business_name = s.business_name
                WHERE s.sent_at >= %s
            """, (cutoff,))
        else:
            cur.execute("""
                SELECT s.status, s.sent_at,
                       COALESCE(b.address, b.location, '') AS addr
                FROM email_sends s
                LEFT JOIN businesses b ON b.business_name = s.business_name
                WHERE DATE(s.sent_at) >= ?
            """, (cutoff.isoformat(),))
        rows = cur.fetchall()

        import re
        # Address format: "STREET, City, ST 12345" (our standard). Some
        # rows only have "Location" like "Austin, TX". Parse flexibly.
        state_re = re.compile(r",\s*([^,]+),\s*([A-Z]{2})(?:\s+\d{5})?", re.I)
        state_counts: dict[str, dict] = {}
        city_counts: dict[str, dict] = {}
        for r in rows:
            addr = (r["addr"] or "").strip()
            if not addr:
                continue
            m = state_re.search(addr)
            if not m:
                # Fallback: "City, ST" with no street prefix
                m2 = re.search(r"^([A-Za-z .'\-]+),\s*([A-Z]{2})\b", addr)
                if not m2:
                    continue
                city, state = m2.group(1).strip(), m2.group(2).upper()
            else:
                city, state = m.group(1).strip(), m.group(2).upper()
            is_bounce = (r["status"] == "bounced")
            # Aggregate by state
            sa = state_counts.setdefault(state, {"sent": 0, "bounced": 0})
            sa["sent"] += 1
            if is_bounce:
                sa["bounced"] += 1
            # Aggregate by city+state
            ck = f"{city}, {state}"
            ca = city_counts.setdefault(ck, {"sent": 0, "bounced": 0})
            ca["sent"] += 1
            if is_bounce:
                ca["bounced"] += 1

        by_state = sorted(
            [{"state": k, **v} for k, v in state_counts.items()],
            key=lambda x: -x["sent"],
        )[:15]
        by_city = sorted(
            [{"city": k, **v} for k, v in city_counts.items()],
            key=lambda x: -x["sent"],
        )[:15]
        return {"by_state": by_state, "by_city": by_city}
    finally:
        conn.close()


def daily_rollup_by_vertical(days: int = 30) -> dict:
    """
    Daily activity broken down by macro vertical. Returns:
      {
        'days': ['YYYY-MM-DD', ...],                 # zero-filled series
        'scrape':   {'Law / Legal': [..per-day..], 'Dental': [..], ...},
        'outreach': {'Law / Legal': [..per-day..], 'Dental': [..], ...},
      }
    Each vertical's list is aligned to `days`. Zero-filled so the chart
    doesn't drop verticals that have gaps.
    """
    init_db()
    cutoff = (datetime.utcnow() - timedelta(days=days)).date()
    day_keys = [str(cutoff + timedelta(days=i)) for i in range(days + 1)]
    day_index = {k: i for i, k in enumerate(day_keys)}

    conn = _connect()
    try:
        cur = _cursor(conn)
        # Scraping side: businesses with primary_email
        if USE_PG:
            cur.execute("""
                SELECT DATE(COALESCE(scraped_at, created_at)) AS day,
                       business_type,
                       COUNT(*) FILTER (WHERE primary_email IS NOT NULL AND primary_email <> '') AS found
                FROM businesses
                WHERE COALESCE(scraped_at, created_at) >= %s
                GROUP BY day, business_type
            """, (cutoff,))
        else:
            cur.execute("""
                SELECT DATE(COALESCE(scraped_at, created_at)) AS day,
                       business_type,
                       SUM(CASE WHEN primary_email IS NOT NULL AND primary_email <> '' THEN 1 ELSE 0 END) AS found
                FROM businesses
                WHERE DATE(COALESCE(scraped_at, created_at)) >= ?
                GROUP BY day, business_type
            """, (cutoff.isoformat(),))
        scrape: dict[str, list[int]] = {}
        for r in cur.fetchall():
            v = normalize_vertical(r["business_type"] or "")
            idx = day_index.get(str(r["day"]))
            if idx is None:
                continue
            if v not in scrape:
                scrape[v] = [0] * len(day_keys)
            scrape[v][idx] += int(r["found"] or 0)

        # Outreach side: email_sends
        if USE_PG:
            cur.execute("""
                SELECT DATE(sent_at) AS day,
                       business_type,
                       COUNT(*) AS sent
                FROM email_sends
                WHERE sent_at >= %s
                GROUP BY day, business_type
            """, (cutoff,))
        else:
            cur.execute("""
                SELECT DATE(sent_at) AS day,
                       business_type,
                       COUNT(*) AS sent
                FROM email_sends
                WHERE DATE(sent_at) >= ?
                GROUP BY day, business_type
            """, (cutoff.isoformat(),))
        outreach: dict[str, list[int]] = {}
        for r in cur.fetchall():
            v = normalize_vertical(r["business_type"] or "")
            idx = day_index.get(str(r["day"]))
            if idx is None:
                continue
            if v not in outreach:
                outreach[v] = [0] * len(day_keys)
            outreach[v][idx] += int(r["sent"] or 0)

        return {"days": day_keys, "scrape": scrape, "outreach": outreach}
    finally:
        conn.close()
