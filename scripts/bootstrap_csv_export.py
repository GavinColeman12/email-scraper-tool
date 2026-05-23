"""One-time CSV export of existing leads from email-scraper Postgres/SQLite
for bulk import into HubSpot.

Output: exports/bootstrap_leads_<date>.csv with columns matching HubSpot's
multi-object import format (Contacts + Companies).

The actual `businesses` table schema (see src/storage.py) doesn't have
industry / num_locations / source_batch columns. The script fills those
custom HubSpot fields with sensible defaults — you can edit individual rows
in the CSV before importing if you want to set them per-row.

Usage:
    cd /Users/gavincoleman/Downloads/email-scraper
    python scripts/bootstrap_csv_export.py
"""

from __future__ import annotations

import csv
import os
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "exports"
OUTPUT_DIR.mkdir(exist_ok=True)


def _connect():
    """Return a DB connection (Postgres if DATABASE_URL set, else SQLite)."""
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        import psycopg2
        return psycopg2.connect(db_url), "pg"
    sqlite_path = (
        Path(__file__).resolve().parent.parent / "data" / "scraper.db"
    )
    if not sqlite_path.exists():
        sys.exit(
            f"No DATABASE_URL set and no SQLite DB at {sqlite_path}. "
            "Set DATABASE_URL in .env or check the SQLite path."
        )
    import sqlite3
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    return conn, "sqlite"


def _split_name(full: str | None) -> tuple[str, str]:
    """Split 'First Last' into ('First', 'Last'). Empty when missing."""
    if not full:
        return "", ""
    parts = full.strip().split(maxsplit=1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def _parse_location(address: str | None, location: str | None) -> tuple[str, str]:
    """Extract (city, state) from address/location strings — best-effort.

    Examples that should work:
        "123 Main St, Austin, TX 78701" -> ("Austin", "TX")
        "Brooklyn, NY"                  -> ("Brooklyn", "NY")
        "Austin"                        -> ("Austin", "")
    """
    src = (address or location or "").strip()
    if not src:
        return "", ""
    parts = [p.strip() for p in src.split(",")]
    if len(parts) >= 3:
        # "street, city, state zip" form
        city = parts[-2]
        state_zip = parts[-1].split()
        state = state_zip[0] if state_zip else ""
        return city, state
    if len(parts) == 2:
        return parts[0], parts[1].split()[0] if parts[1] else ""
    return parts[0], ""


def fetch_leads(conn, dialect: str) -> list[dict]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            business_name,
            website,
            primary_email,
            contact_name,
            contact_title,
            phone,
            address,
            location,
            search_id
        FROM businesses
        WHERE primary_email IS NOT NULL
          AND primary_email != ''
        ORDER BY id
        """
    )
    rows = cur.fetchall()
    if dialect == "pg":
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in rows]
    return [dict(row) for row in rows]


def write_csv(leads: list[dict]) -> Path:
    out_path = OUTPUT_DIR / f"bootstrap_leads_{date.today().isoformat()}.csv"
    fieldnames = [
        "Company name",
        "Website URL",
        "Email",
        "First Name",
        "Last Name",
        "Job Title",
        "Phone Number",
        "City",
        "State/Region",
        "Industry",
        "# of Locations",
        "Source / List Batch",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for lead in leads:
            first, last = _split_name(lead.get("contact_name"))
            city, state = _parse_location(lead.get("address"), lead.get("location"))
            writer.writerow(
                {
                    "Company name": lead["business_name"],
                    "Website URL": lead.get("website") or "",
                    "Email": lead["primary_email"],
                    "First Name": first,
                    "Last Name": last,
                    "Job Title": lead.get("contact_title") or "",
                    "Phone Number": lead.get("phone") or "",
                    "City": city,
                    "State/Region": state,
                    "Industry": "Other",  # No column in DB; edit CSV manually if needed
                    "# of Locations": 1,       # Default; edit CSV manually if known
                    "Source / List Batch": f"pre-hubspot-search-{lead.get('search_id') or 'unknown'}",
                }
            )
    return out_path


if __name__ == "__main__":
    conn, dialect = _connect()
    try:
        leads = fetch_leads(conn, dialect)
    finally:
        conn.close()
    out = write_csv(leads)
    print(f"Wrote {len(leads)} leads to {out}")
    print(f"Spot-check the CSV before importing: head -3 {out}")
