#!/usr/bin/env python3
"""Batch scrape London businesses across hospitality + professional services
verticals for Joanna's UK pipeline.

Workflow per vertical:
  1. Create a search row in the searches table
  2. Run Google Maps search (max 50 results per vertical)
  3. Bulk-add results to the businesses table
  4. For each business with a website: run volume_mode scraper
     (finds emails, NeverBounce-verifies, scores), persist results
  5. Re-score with the latest data

Cost ceiling per vertical is enforced by the existing $25 volume-mode
budget; the script resets it per vertical, so each vertical gets its
own envelope. Realistic total spend for 13 verticals × ~50 leads:
~$15–25 across SearchAPI + NeverBounce.

Usage:
    cd /Users/gavincoleman/Downloads/email-scraper
    python3 scripts/london_batch_scrape.py

    # Customize verticals at the top of this file
    # Customize max_results per vertical via env: VERTICAL_MAX_RESULTS=30
    # Dry run (just map searches, skip the volume scrape): DRY_RUN=1

Re-run safe: each invocation creates new search rows. Duplicate businesses
(same place_id) are deduped by storage.add_business — no double-write.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

# Make `from src...` work when run from anywhere
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from src import storage, maps_search
from src.volume_mode import scrape_volume
from src.volume_mode.pipeline import reset_run_budget, volume_result_to_scrape_result
from src.lead_scoring import compute_lead_quality_score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("london_batch")


# Verticals to target — hospitality-heavy + professional services.
# Joanna's focus is London, so all queries pair with London as location.
VERTICALS = [
    # Hospitality
    "restaurant",
    "cafe",
    "bar",
    "gastropub",
    "hotel",
    "bed and breakfast",
    "coffee shop",
    # Professional services
    "law firm",
    "solicitor",
    "chartered accountant",
    "marketing agency",
    "management consultant",
    "boutique consultancy",
]

LOCATION = "London, UK"
MAX_RESULTS_PER_VERTICAL = int(os.environ.get("VERTICAL_MAX_RESULTS", "50"))
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
PER_VERTICAL_BUDGET = float(os.environ.get("PER_VERTICAL_BUDGET", "25.0"))


def scrape_one_vertical(vertical: str) -> dict:
    """Returns a stats dict for the vertical: {searched, scraped, errored}."""
    log.info("=" * 60)
    log.info("Vertical: %r in %s", vertical, LOCATION)

    # Step 1: Google Maps search
    search_id = storage.create_search(
        query=vertical, location=LOCATION, max_results=MAX_RESULTS_PER_VERTICAL
    )
    log.info("Created search_id=%d", search_id)

    try:
        results = maps_search.search_businesses(
            query=vertical, location=LOCATION, max_results=MAX_RESULTS_PER_VERTICAL
        )
    except Exception as exc:
        log.error("Maps search failed for %r: %s", vertical, exc)
        return {"vertical": vertical, "searched": 0, "scraped": 0, "errored": 1}

    log.info("Maps returned %d businesses", len(results))

    if not results:
        return {"vertical": vertical, "searched": 0, "scraped": 0, "errored": 0}

    # Step 2: Add to DB
    added = storage.add_businesses_bulk(search_id, results)
    log.info("Inserted %d new businesses into Neon (deduped by place_id)", added)

    if DRY_RUN:
        log.info("[DRY_RUN] Skipping volume scrape.")
        return {"vertical": vertical, "searched": len(results), "scraped": 0, "errored": 0}

    # Step 3: Volume scrape each business
    reset_run_budget(PER_VERTICAL_BUDGET)
    businesses = storage.list_businesses(search_id=search_id)
    businesses = [b for b in businesses if b.get("website")]
    log.info("Volume-scraping %d businesses with websites...", len(businesses))

    scraped = 0
    errored = 0
    for i, biz in enumerate(businesses, 1):
        name = biz.get("business_name", "?")[:40]
        try:
            vres = scrape_volume(
                biz,
                use_neverbounce=True,
                rescue_empties_with_searchapi=True,
            )
            result = volume_result_to_scrape_result(vres, biz)
            storage.update_business_emails(biz["id"], result)

            # Re-fetch + score
            fresh = storage.list_businesses(search_id=search_id)
            updated = next((b for b in fresh if b["id"] == biz["id"]), None)
            if updated:
                s = compute_lead_quality_score(updated)
                storage.update_lead_score(
                    biz["id"],
                    s["score"],
                    s["tier"],
                    all_emails=result.get("scraped_emails", []),
                )
                email = result.get("primary_email") or "(no email)"
                log.info("  [%d/%d] %s → %s  tier=%s",
                         i, len(businesses), name, email, s["tier"])
            scraped += 1
        except Exception as exc:
            errored += 1
            log.warning("  [%d/%d] %s — ERROR: %s", i, len(businesses), name, exc)
        # Light throttle so we don't hammer external APIs
        time.sleep(0.5)

    return {"vertical": vertical, "searched": len(results), "scraped": scraped, "errored": errored}


def main() -> int:
    if "DATABASE_URL" not in os.environ:
        log.error("DATABASE_URL not set in .env — aborting")
        return 1

    log.info("London batch scrape starting")
    log.info("  Verticals: %d", len(VERTICALS))
    log.info("  Max per vertical: %d", MAX_RESULTS_PER_VERTICAL)
    log.info("  Budget per vertical: $%.2f", PER_VERTICAL_BUDGET)
    log.info("  Dry run: %s", DRY_RUN)
    log.info("")

    start = time.time()
    all_stats = []
    for vertical in VERTICALS:
        try:
            stats = scrape_one_vertical(vertical)
        except KeyboardInterrupt:
            log.warning("Interrupted by user — stopping after %r", vertical)
            break
        except Exception as exc:
            log.exception("Vertical %r failed completely: %s", vertical, exc)
            stats = {"vertical": vertical, "searched": 0, "scraped": 0, "errored": 1}
        all_stats.append(stats)
        log.info("")

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info("DONE. Elapsed: %d min %d sec", int(elapsed // 60), int(elapsed % 60))
    log.info("")
    log.info("Per-vertical summary:")
    log.info("  %-25s %8s %8s %8s", "Vertical", "Found", "Scraped", "Errors")
    log.info("  %-25s %8s %8s %8s", "-" * 25, "-" * 8, "-" * 8, "-" * 8)
    totals = {"searched": 0, "scraped": 0, "errored": 0}
    for s in all_stats:
        log.info("  %-25s %8d %8d %8d",
                 s["vertical"], s["searched"], s["scraped"], s["errored"])
        totals["searched"] += s["searched"]
        totals["scraped"] += s["scraped"]
        totals["errored"] += s["errored"]
    log.info("  %-25s %8s %8s %8s", "-" * 25, "-" * 8, "-" * 8, "-" * 8)
    log.info("  %-25s %8d %8d %8d",
             "TOTAL", totals["searched"], totals["scraped"], totals["errored"])

    log.info("")
    log.info("Next step: re-run the HubSpot bootstrap export to pick up new leads")
    log.info("  python3 scripts/bootstrap_csv_export.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
