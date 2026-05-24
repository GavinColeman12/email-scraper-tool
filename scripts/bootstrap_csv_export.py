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


def _classify_vertical(business_type: str | None) -> str:
    """Map the scraper's free-text business_type (from Google Maps) to one of
    our 10 Business Vertical dropdown values. Best-effort; defaults to 'Other'.
    """
    if not business_type:
        return "Other"
    t = business_type.lower()
    if any(w in t for w in ("restaurant", "pizza", "diner", "eatery", "bistro", "grill", "steakhouse")):
        return "Restaurant"
    if any(w in t for w in ("cafe", "coffee", "bar", "brewery", "pub", "tavern", "lounge")):
        return "Cafe / Bar / Brewery"
    if any(w in t for w in ("hotel", "motel", "inn", "lodge", "b&b", "bed and breakfast", "hostel")):
        return "Hospitality (hotel, B&B)"
    if any(w in t for w in ("retail", "store", "shop", "boutique", "ecommerce", "e-commerce")):
        return "Retail / E-commerce"
    if any(w in t for w in ("salon", "spa", "gym", "fitness", "yoga", "pilates", "barber", "nail")):
        return "Health & Wellness (salon, spa, gym)"
    # Specific verticals BEFORE generic Professional Services
    if any(w in t for w in ("realtor", "real estate", "realty", "broker", "property management")):
        return "Real Estate"
    if any(w in t for w in ("dentist", "dental", "doctor", "clinic", "medical", "physician", "dermatolog", "chiropract", "veterinarian", "vet ")):
        return "Healthcare / Dental"
    if any(w in t for w in ("plumber", "electrician", "contractor", "landscap", "roofing", "hvac", "construction", "remodel", "painter")):
        return "Home Services"
    if any(w in t for w in ("law", "attorney", "lawyer", "esq", "consultant", "accountant", "cpa", "agency", "marketing", "advertising")):
        return "Professional Services"
    return "Other"


def _extract_linkedin_url(professional_ids) -> str:
    """Pull a LinkedIn URL out of the professional_ids JSON column if present.

    The scraper stores the decision-maker's source URL in
    professional_ids.decision_maker.source_url. When source = 'linkedin_via_google',
    that URL is the person's LinkedIn profile.
    """
    import json
    if not professional_ids:
        return ""
    try:
        data = json.loads(professional_ids) if isinstance(professional_ids, str) else professional_ids
        dm = data.get("decision_maker") or {}
        src = (dm.get("source") or "").lower()
        url = dm.get("source_url") or ""
        if "linkedin.com" in url.lower():
            return url
        # Sometimes it's nested in all_providers
        for prov in data.get("all_providers") or []:
            u = prov.get("source_url") or ""
            if "linkedin.com" in u.lower():
                return u
    except Exception:
        pass
    return ""


def _categorize_email_source(email_source: str | None) -> str:
    """Bucket the verbose email_source string into 3 sales-useful categories.

    Direct scrape  = email was literally found on the company's website
                     (highest confidence — definitely real)
    Pattern        = email was constructed from a known industry pattern
                     ({first}.{last}, etc.) and NeverBounce-verified valid
    Triangulated   = constructed from multiple evidence sources and verified
    """
    if not email_source:
        return "Unknown"
    src = email_source.lower()
    if "scraped from website" in src:
        return "Direct scrape"
    if "industry prior" in src or "pattern" in src and "triangulated" not in src:
        return "Pattern"
    if "triangulated" in src:
        return "Triangulated"
    if "cross-verified" in src:
        return "Cross-verified"
    if "rescued" in src:
        return "Rescued"
    return "Other"


def _build_description(lead: dict) -> str:
    """Pack the data that doesn't fit in a custom property into a single
    multi-line string for HubSpot's standard Company `description` field.

    Output is human-readable AND machine-parseable (each line starts with
    a stable prefix like 'Lead Quality:' / 'Google:').
    """
    lines: list[str] = []

    tier = lead.get("lead_tier") or ""
    score = lead.get("lead_quality_score")
    if tier or score is not None:
        score_str = f"({score}/100)" if score is not None else ""
        lines.append(f"Lead Quality: Tier {tier or '?'} {score_str}".strip())

    email_src = lead.get("email_source")
    if email_src:
        cat = _categorize_email_source(email_src)
        lines.append(f"Email Source: {cat} — {email_src}")

    rating = lead.get("rating")
    reviews = lead.get("review_count")
    maps_url = lead.get("google_maps_url")
    if rating or reviews or maps_url:
        parts = []
        if rating:
            parts.append(f"{rating}★")
        if reviews:
            parts.append(f"{reviews} reviews")
        if maps_url:
            parts.append(maps_url)
        lines.append("Google: " + " · ".join(parts))

    return "\n".join(lines)


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
    """Pull the 'best cohort' of leads worth importing into HubSpot Free.

    Filter (matches Gavin's mental model: high quality + confirmed email +
    confirmed decision maker):
      - lead_tier IN ('A', 'B')
      - confidence IN ('high', 'very_high', 'confirmed', 'verified')
      - contact_title matches owner/founder/CEO/president/principal/partner

    This narrows ~2,400 raw eligible leads down to ~300 — well under
    HubSpot Free's 1,000 contact cap, with headroom for new leads.

    To override (export everyone), set env var BOOTSTRAP_FILTER=off.
    """
    cur = conn.cursor()
    filter_mode = os.environ.get("BOOTSTRAP_FILTER", "best").lower()

    if filter_mode == "off":
        where_clause = "primary_email IS NOT NULL AND primary_email != ''"
    else:
        where_clause = """
            primary_email IS NOT NULL AND primary_email != ''
            AND lead_tier IN ('A', 'B')
            AND LOWER(COALESCE(confidence, '')) IN ('high', 'very_high', 'confirmed', 'verified')
            AND LOWER(COALESCE(contact_title, '')) ~ '(owner|founder|ceo|president|principal|partner)'
        """

    cur.execute(
        f"""
        SELECT
            business_name,
            website,
            primary_email,
            contact_name,
            contact_title,
            phone,
            address,
            location,
            search_id,
            business_type,
            lead_tier,
            confidence,
            email_source,
            rating,
            review_count,
            google_maps_url,
            lead_quality_score,
            professional_ids
        FROM businesses
        WHERE {where_clause}
        ORDER BY
            CASE lead_tier WHEN 'A' THEN 0 WHEN 'B' THEN 1 ELSE 2 END,
            id
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
        "LinkedIn URL",
        "City",
        "State/Region",
        "Business Vertical",
        "# of Locations",
        "Source / List Batch",
        "Email Source Category",
        "Company Description",  # packed: tier + score + email source + Google
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
                    "LinkedIn URL": _extract_linkedin_url(lead.get("professional_ids")),
                    "City": city,
                    "State/Region": state,
                    "Business Vertical": _classify_vertical(lead.get("business_type")),
                    "# of Locations": 1,       # Default; edit CSV manually if known
                    "Source / List Batch": f"pre-hubspot-search-{lead.get('search_id') or 'unknown'}",
                    "Email Source Category": _categorize_email_source(lead.get("email_source")),
                    "Company Description": _build_description(lead),
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
