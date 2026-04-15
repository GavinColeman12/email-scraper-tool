"""
SQLite storage for scraped businesses + emails.
Simple single-table design — each row is one business with its primary email.
"""
import json
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "scraper.db"


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.executescript("""
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
        """)


def create_search(query: str, location: str, max_results: int) -> int:
    init_db()
    with _connect() as conn:
        cur = conn.execute(
            "INSERT INTO searches (query, location, max_results) VALUES (?, ?, ?)",
            (query, location, max_results),
        )
        return cur.lastrowid


def list_searches() -> list:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT s.*, COUNT(b.id) as business_count, "
            "       SUM(CASE WHEN b.primary_email IS NOT NULL AND b.primary_email != '' THEN 1 ELSE 0 END) as with_email "
            "FROM searches s LEFT JOIN businesses b ON b.search_id = s.id "
            "GROUP BY s.id ORDER BY s.created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def get_search(search_id: int) -> dict:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM searches WHERE id = ?", (search_id,)).fetchone()
        return dict(row) if row else None


def delete_search(search_id: int) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM searches WHERE id = ?", (search_id,))


def add_business(search_id: int, business: dict) -> int:
    with _connect() as conn:
        cur = conn.execute("""
            INSERT INTO businesses
                (search_id, business_name, business_type, address, location,
                 phone, website, rating, review_count, place_id, google_maps_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
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
        ))
        return cur.lastrowid


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
        where.append("search_id = ?")
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

    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [_parse(r) for r in rows]


def _parse(row) -> dict:
    if row is None:
        return None
    d = dict(row)
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
    """Persist scraping results to a business row."""
    with _connect() as conn:
        conn.execute("""
            UPDATE businesses SET
                primary_email = ?,
                scraped_emails_json = ?,
                constructed_emails_json = ?,
                contact_name = ?,
                scraped_at = ?
            WHERE id = ?
        """, (
            scrape_result.get("primary_email", ""),
            json.dumps(scrape_result.get("scraped_emails", [])),
            json.dumps(scrape_result.get("constructed_emails", [])),
            (scrape_result.get("contact_names") or [{}])[0].get("full", "") if scrape_result.get("contact_names") else "",
            datetime.utcnow().isoformat(),
            business_id,
        ))


def update_business_verification(business_id: int, status: str, reason: str = "") -> None:
    with _connect() as conn:
        conn.execute("""
            UPDATE businesses SET
                email_status = ?,
                email_verification_reason = ?,
                verified_at = ?
            WHERE id = ?
        """, (status, reason, datetime.utcnow().isoformat(), business_id))


def override_primary_email(business_id: int, new_email: str) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE businesses SET primary_email = ? WHERE id = ?",
            (new_email, business_id),
        )


def stats(search_id: int = None) -> dict:
    init_db()
    where = "WHERE search_id = ?" if search_id else ""
    params = (search_id,) if search_id else ()
    with _connect() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM businesses {where}", params
        ).fetchone()[0]
        with_email = conn.execute(
            f"SELECT COUNT(*) FROM businesses {where} "
            + ("AND " if where else "WHERE ")
            + "primary_email IS NOT NULL AND primary_email != ''",
            params,
        ).fetchone()[0]
        verified = conn.execute(
            f"SELECT COUNT(*) FROM businesses {where} "
            + ("AND " if where else "WHERE ")
            + "email_status = 'valid'",
            params,
        ).fetchone()[0]
    return {
        "total": total,
        "with_email": with_email,
        "verified": verified,
    }
