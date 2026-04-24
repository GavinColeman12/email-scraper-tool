"""
Storage for scraped businesses + emails.

Uses Postgres (via DATABASE_URL env var) if set, otherwise falls back to
local SQLite at data/scraper.db. Same public API either way.
"""
import json
from pathlib import Path
from datetime import datetime

from src.secrets import get_secret

DB_URL = get_secret("DATABASE_URL") or ""
USE_PG = DB_URL.startswith("postgres")

if USE_PG:
    import psycopg2
    import psycopg2.extras
    _PARAM = "%s"
else:
    import sqlite3
    _PARAM = "?"
    DB_PATH = Path(__file__).resolve().parent.parent / "data" / "scraper.db"


# ── Connection helpers ────────────────────────────────────────────────

def _connect():
    if USE_PG:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        return conn
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _cursor(conn):
    if USE_PG:
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn.cursor()


def _row_to_dict(row) -> dict:
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


# ── Schema ────────────────────────────────────────────────────────────

SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS searches (
    id SERIAL PRIMARY KEY,
    query TEXT NOT NULL,
    location TEXT,
    max_results INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS businesses (
    id SERIAL PRIMARY KEY,
    search_id INTEGER REFERENCES searches(id) ON DELETE CASCADE,
    business_name TEXT NOT NULL,
    business_type TEXT,
    address TEXT,
    location TEXT,
    phone TEXT,
    website TEXT,
    rating REAL,
    review_count INTEGER,
    place_id TEXT,
    google_maps_url TEXT,
    primary_email TEXT,
    scraped_emails_json TEXT,
    constructed_emails_json TEXT,
    contact_name TEXT,
    contact_title TEXT,
    email_source TEXT,
    confidence TEXT,
    email_status TEXT,
    email_verification_reason TEXT,
    scraped_at TIMESTAMP,
    verified_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_businesses_search ON businesses(search_id);
CREATE INDEX IF NOT EXISTS idx_businesses_place ON businesses(place_id);
CREATE INDEX IF NOT EXISTS idx_businesses_email ON businesses(primary_email);
"""

SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    location TEXT,
    max_results INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS businesses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id INTEGER,
    business_name TEXT NOT NULL,
    business_type TEXT,
    address TEXT,
    location TEXT,
    phone TEXT,
    website TEXT,
    rating REAL,
    review_count INTEGER,
    place_id TEXT,
    google_maps_url TEXT,
    primary_email TEXT,
    scraped_emails_json TEXT,
    constructed_emails_json TEXT,
    contact_name TEXT,
    contact_title TEXT,
    email_source TEXT,
    confidence TEXT,
    email_status TEXT,
    email_verification_reason TEXT,
    scraped_at TIMESTAMP,
    verified_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (search_id) REFERENCES searches(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_businesses_search ON businesses(search_id);
CREATE INDEX IF NOT EXISTS idx_businesses_place ON businesses(place_id);
CREATE INDEX IF NOT EXISTS idx_businesses_email ON businesses(primary_email);
"""


_INITIALIZED = False


def init_db() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return
    conn = _connect()
    try:
        cur = _cursor(conn)
        if USE_PG:
            cur.execute(SCHEMA_PG)
        else:
            conn.executescript(SCHEMA_SQLITE)
        conn.commit()
        # Idempotent migrations for existing deployments
        for col, typ in [
            ("contact_title", "TEXT"),
            ("email_source", "TEXT"),
            ("confidence", "TEXT"),
            ("reasoning", "TEXT"),
            ("synthesizer", "TEXT"),
            ("lead_quality_score", "INTEGER"),
            ("lead_tier", "TEXT"),
            ("deliverability_score", "INTEGER"),
            ("all_found_emails_json", "TEXT"),
            ("hidden_emails_json", "TEXT"),
            # New for waterfall + pattern tracking
            ("pattern_used", "TEXT"),
            ("neverbounce_result", "TEXT"),
            ("waterfall_verdict", "TEXT"),
            ("waterfall_confidence", "INTEGER"),
            ("headcount", "INTEGER"),
            # New for v3 triangulation pipeline
            ("professional_ids", "TEXT"),
            ("triangulation_pattern", "TEXT"),
            ("triangulation_confidence", "INTEGER"),
            ("triangulation_method", "TEXT"),
            ("email_safe_to_send", "INTEGER DEFAULT 0"),
        ]:
            try:
                if USE_PG:
                    cur.execute(f"ALTER TABLE businesses ADD COLUMN IF NOT EXISTS {col} {typ}")
                else:
                    cur.execute(f"ALTER TABLE businesses ADD COLUMN {col} {typ}")
                conn.commit()
            except Exception:
                conn.rollback() if USE_PG else None
        _INITIALIZED = True
    finally:
        conn.close()

    # Initialize bounce tracking tables too (idempotent)
    try:
        from src.bounce_tracker import init_bounce_tables
        init_bounce_tables()
    except Exception:
        pass  # Don't fail startup if bounce tables can't be created


# ── Searches ──────────────────────────────────────────────────────────

def create_search(query: str, location: str, max_results: int) -> int:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        if USE_PG:
            cur.execute(
                "INSERT INTO searches (query, location, max_results) "
                "VALUES (%s, %s, %s) RETURNING id",
                (query, location, max_results),
            )
            new_id = cur.fetchone()["id"]
        else:
            cur.execute(
                "INSERT INTO searches (query, location, max_results) VALUES (?, ?, ?)",
                (query, location, max_results),
            )
            new_id = cur.lastrowid
        conn.commit()
        return new_id
    finally:
        conn.close()


def list_searches() -> list:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            "SELECT s.id, s.query, s.location, s.max_results, s.created_at, "
            "       COUNT(b.id) as business_count, "
            "       SUM(CASE WHEN b.primary_email IS NOT NULL "
            "               AND b.primary_email != '' THEN 1 ELSE 0 END) as with_email "
            "FROM searches s LEFT JOIN businesses b ON b.search_id = s.id "
            "GROUP BY s.id ORDER BY s.created_at DESC"
        )
        rows = cur.fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_search(search_id: int) -> dict:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(f"SELECT * FROM searches WHERE id = {_PARAM}", (search_id,))
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def delete_search(search_id: int) -> None:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(f"DELETE FROM searches WHERE id = {_PARAM}", (search_id,))
        conn.commit()
    finally:
        conn.close()


# ── Businesses ────────────────────────────────────────────────────────

def add_business(search_id: int, business: dict) -> int:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        params = (
            search_id,
            business.get("business_name", ""),
            business.get("business_type", ""),
            business.get("address", ""),
            business.get("location", ""),
            business.get("phone", ""),
            business.get("website", ""),
            float(business.get("rating") or 0),
            int(business.get("review_count") or 0),
            business.get("place_id", ""),
            business.get("google_maps_url", ""),
        )
        if USE_PG:
            cur.execute("""
                INSERT INTO businesses
                    (search_id, business_name, business_type, address, location,
                     phone, website, rating, review_count, place_id, google_maps_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, params)
            new_id = cur.fetchone()["id"]
        else:
            cur.execute("""
                INSERT INTO businesses
                    (search_id, business_name, business_type, address, location,
                     phone, website, rating, review_count, place_id, google_maps_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, params)
            new_id = cur.lastrowid
        conn.commit()
        return new_id
    finally:
        conn.close()


def add_businesses_bulk(search_id: int, businesses: list) -> int:
    count = 0
    for b in businesses:
        if b.get("business_name"):
            add_business(search_id, b)
            count += 1
    return count


def list_businesses(search_id: int = None, has_email: bool = None,
                    verified_only: bool = False) -> list:
    init_db()
    where = []
    params = []
    if search_id is not None:
        where.append(f"search_id = {_PARAM}")
        params.append(search_id)
    if has_email is True:
        where.append("primary_email IS NOT NULL AND primary_email != ''")
    elif has_email is False:
        where.append("(primary_email IS NULL OR primary_email = '')")
    if verified_only:
        where.append("email_status = 'valid'")

    sql = "SELECT * FROM businesses"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"

    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [_parse(r) for r in rows]
    finally:
        conn.close()


def _parse(row) -> dict:
    if row is None:
        return None
    d = _row_to_dict(row)
    for field in ("scraped_emails_json", "constructed_emails_json"):
        if d.get(field):
            try:
                d[field.replace("_json", "")] = json.loads(d[field])
            except Exception:
                d[field.replace("_json", "")] = []
        else:
            d[field.replace("_json", "")] = []
    return d


def update_business_emails(business_id: int, scrape_result: dict) -> None:
    init_db()
    # Prefer the top-contact name from new-style result; fall back to
    # first website-extracted name for backward compat.
    contact = scrape_result.get("contact_name", "")
    if not contact and scrape_result.get("contact_names"):
        contact = (scrape_result["contact_names"][0] or {}).get("full", "")
    conn = _connect()
    try:
        cur = _cursor(conn)
        # Normalize the NB verdict. Accepts either the dedicated
        # neverbounce_result key OR parses it out of email_source if
        # that's all the caller passed (triangulation_pattern ==
        # NeverBounce suffix style). The learned-priors aggregator
        # relies on this column being populated going forward.
        nb_val = (scrape_result.get("neverbounce_result") or "").lower().strip()
        if not nb_val:
            import re as _re_nb
            src = scrape_result.get("email_source") or ""
            m = _re_nb.search(
                r"NeverBounce\s+(VALID|CATCH-ALL|UNKNOWN|INVALID)",
                src, _re_nb.IGNORECASE,
            )
            if m:
                nb_val = m.group(1).lower().replace("catch-all", "catchall")
        sql = f"""
            UPDATE businesses SET
                primary_email = {_PARAM},
                scraped_emails_json = {_PARAM},
                constructed_emails_json = {_PARAM},
                contact_name = {_PARAM},
                contact_title = {_PARAM},
                email_source = {_PARAM},
                confidence = {_PARAM},
                reasoning = {_PARAM},
                synthesizer = {_PARAM},
                scraped_at = {_PARAM},
                professional_ids = {_PARAM},
                triangulation_pattern = {_PARAM},
                triangulation_confidence = {_PARAM},
                triangulation_method = {_PARAM},
                email_safe_to_send = {_PARAM},
                neverbounce_result = {_PARAM}
            WHERE id = {_PARAM}
        """
        cur.execute(sql, (
            scrape_result.get("primary_email", ""),
            json.dumps(scrape_result.get("scraped_emails", [])),
            json.dumps(scrape_result.get("constructed_emails", [])),
            contact,
            scrape_result.get("contact_title", ""),
            scrape_result.get("email_source", ""),
            scrape_result.get("confidence", ""),
            scrape_result.get("synthesis_reasoning", ""),
            scrape_result.get("synthesizer", ""),
            datetime.utcnow().isoformat(),
            scrape_result.get("professional_ids_json") or None,
            scrape_result.get("triangulation_pattern") or None,
            scrape_result.get("triangulation_confidence") or None,
            scrape_result.get("triangulation_method") or None,
            1 if scrape_result.get("email_safe_to_send") else 0,
            nb_val or None,
            business_id,
        ))
        conn.commit()
    finally:
        conn.close()


def update_business_verification(business_id: int, status: str, reason: str = "") -> None:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(f"""
            UPDATE businesses SET
                email_status = {_PARAM},
                email_verification_reason = {_PARAM},
                verified_at = {_PARAM}
            WHERE id = {_PARAM}
        """, (status, reason, datetime.utcnow().isoformat(), business_id))
        conn.commit()
    finally:
        conn.close()


def apply_rescue_upgrade(
    business_id: int,
    new_email: str,
    new_nb_result: str,
    *,
    confidence: str = "high",
) -> None:
    """
    Persist a successful rescue: swap primary_email, refresh the NB
    verdict + timestamp, and flag safe_to_send. Leaves every other
    column untouched — contact_name, contact_title, evidence trail,
    triangulation data, lead score, etc. all preserved.

    update_business_emails() is a full-row overwrite designed for
    fresh scrapes, not partial updates; calling it with a minimal
    dict nulls out the evidence columns. This function is the
    focused alternative for the rescue pass.
    """
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(f"""
            UPDATE businesses SET
                primary_email = {_PARAM},
                neverbounce_result = {_PARAM},
                email_safe_to_send = {_PARAM},
                confidence = {_PARAM},
                scraped_at = {_PARAM}
            WHERE id = {_PARAM}
        """, (
            new_email,
            (new_nb_result or "").lower().strip() or None,
            1 if (new_nb_result or "").lower().strip() == "valid" else 0,
            confidence,
            datetime.utcnow().isoformat(),
            business_id,
        ))
        conn.commit()
    finally:
        conn.close()


def override_primary_email(business_id: int, new_email: str) -> None:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"UPDATE businesses SET primary_email = {_PARAM} WHERE id = {_PARAM}",
            (new_email, business_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_lead_score(business_id: int, score: int, tier: str,
                       deliverability: int = None,
                       all_emails: list = None,
                       hidden_emails: dict = None) -> None:
    """Store computed lead quality + optional deliverability + source breakdown."""
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        sets = [
            f"lead_quality_score = {_PARAM}",
            f"lead_tier = {_PARAM}",
        ]
        params = [int(score or 0), str(tier or "")]
        if deliverability is not None:
            sets.append(f"deliverability_score = {_PARAM}")
            params.append(int(deliverability))
        if all_emails is not None:
            sets.append(f"all_found_emails_json = {_PARAM}")
            params.append(json.dumps(all_emails))
        if hidden_emails is not None:
            sets.append(f"hidden_emails_json = {_PARAM}")
            params.append(json.dumps(hidden_emails))
        params.append(business_id)
        cur.execute(
            f"UPDATE businesses SET {', '.join(sets)} WHERE id = {_PARAM}",
            params,
        )
        conn.commit()
    finally:
        conn.close()


def existing_place_ids() -> set:
    """Return all place_ids already saved (across every search).

    Used by the Find Businesses page to skip duplicates the next time
    you run a search, so you never rescrape businesses you've already seen.
    """
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT DISTINCT place_id FROM businesses WHERE place_id IS NOT NULL AND place_id != ''")
        rows = cur.fetchall()
    finally:
        conn.close()
    out = set()
    for r in rows:
        d = _row_to_dict(r) if USE_PG else {"place_id": r[0]}
        pid = d.get("place_id")
        if pid:
            out.add(pid)
    return out


def stats(search_id: int = None) -> dict:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        if search_id:
            base_where = f"WHERE search_id = {_PARAM}"
            params = (search_id,)
        else:
            base_where = ""
            params = ()

        cur.execute(f"SELECT COUNT(*) AS c FROM businesses {base_where}", params)
        total_row = cur.fetchone()
        total = total_row["c"] if USE_PG else total_row[0]

        where_email = base_where + (" AND " if base_where else "WHERE ") + \
            "primary_email IS NOT NULL AND primary_email != ''"
        cur.execute(f"SELECT COUNT(*) AS c FROM businesses {where_email}", params)
        we_row = cur.fetchone()
        with_email = we_row["c"] if USE_PG else we_row[0]

        where_verified = base_where + (" AND " if base_where else "WHERE ") + \
            "email_status = 'valid'"
        cur.execute(f"SELECT COUNT(*) AS c FROM businesses {where_verified}", params)
        v_row = cur.fetchone()
        verified = v_row["c"] if USE_PG else v_row[0]

    finally:
        conn.close()
    return {
        "total": total or 0,
        "with_email": with_email or 0,
        "verified": verified or 0,
    }
