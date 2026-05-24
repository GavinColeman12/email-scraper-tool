#!/usr/bin/env python3
"""Backfill HubSpot sync for businesses already in Neon but not yet pushed
to HubSpot. Use when:
  - A scrape happened before HUBSPOT_SYNC_ENABLED was true
  - A scrape ran via a code path that wasn't yet instrumented
  - You want to retry leads that errored during live sync

Default: syncs ALL leads matching the bootstrap "best" cohort filter
(strict OR LinkedIn confirmed). Skips leads already in HubSpot (upsert).

Usage:
    cd /Users/gavincoleman/Downloads/email-scraper
    python3 scripts/backfill_hubspot_sync.py

    # Limit to recent leads only (e.g., last hour)
    BACKFILL_HOURS=1 python3 scripts/backfill_hubspot_sync.py

    # Limit to a specific search
    BACKFILL_SEARCH_ID=123 python3 scripts/backfill_hubspot_sync.py
"""

from __future__ import annotations
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import psycopg2
from src.hubspot_sync import sync_lead_to_hubspot
from src.lead_scoring import compute_lead_quality_score

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("backfill")


def main() -> int:
    if "DATABASE_URL" not in os.environ:
        log.error("DATABASE_URL not set in .env")
        return 1
    if "HUBSPOT_ACCESS_TOKEN" not in os.environ:
        log.error("HUBSPOT_ACCESS_TOKEN not set in .env")
        return 1

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()

    # Same "best cohort" filter as bootstrap_csv_export.py
    where = """
        primary_email IS NOT NULL AND primary_email != ''
        AND LOWER(COALESCE(contact_title, '')) ~ '(owner|founder|ceo|president|principal|partner)'
        AND (
            professional_ids::text ILIKE '%linkedin.com%'
            OR (lead_tier IN ('A', 'B')
                AND LOWER(COALESCE(confidence, '')) IN ('high', 'very_high', 'confirmed', 'verified'))
        )
    """

    # Optional filters
    hours = os.environ.get("BACKFILL_HOURS")
    if hours:
        where += f" AND scraped_at > NOW() - INTERVAL '{int(hours)} hour'"

    search_id = os.environ.get("BACKFILL_SEARCH_ID")
    if search_id:
        where += f" AND search_id = {int(search_id)}"

    cur.execute(f"""
        SELECT DISTINCT ON (primary_email) *
        FROM businesses
        WHERE {where}
        ORDER BY primary_email,
                 CASE lead_tier WHEN 'A' THEN 0 WHEN 'B' THEN 1 ELSE 2 END,
                 id
    """)

    cols = [c.name for c in cur.description]
    leads = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()

    log.info("Backfilling %d leads to HubSpot...", len(leads))
    ok = 0
    err = 0
    for i, lead in enumerate(leads, 1):
        name = (lead.get("business_name") or "?")[:40]
        # Compute score on the fly
        try:
            score_data = compute_lead_quality_score(lead)
            score = score_data["score"]
        except Exception as exc:
            log.warning("[%d/%d] %s — couldn't score: %s", i, len(leads), name, exc)
            err += 1
            continue

        result = sync_lead_to_hubspot(business=lead, lead_score_0_100=score)
        if result.error:
            log.warning("[%d/%d] %s — sync error: %s", i, len(leads), name, result.error)
            err += 1
        else:
            ok += 1
            log.info("[%d/%d] %s → company=%s contact=%s deal=%s",
                     i, len(leads), name, result.company_id, result.contact_id, result.deal_id)

    log.info("=" * 60)
    log.info("Done. Synced %d / Errors %d / Total %d", ok, err, len(leads))
    return 0


if __name__ == "__main__":
    sys.exit(main())
