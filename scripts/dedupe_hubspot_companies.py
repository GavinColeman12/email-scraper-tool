#!/usr/bin/env python3
"""Find duplicate companies in HubSpot (by domain) and merge them.

The CSV bulk import created ~470 duplicate companies because HubSpot's
'Update existing records' toggle was greyed out for Companies. The live
sync (upsert_company) dedupes correctly going forward, but the bulk-import
mess needs cleanup.

For each duplicate group (>1 company with same domain):
  1. Pick the 'primary' = company with the most populated standard fields
  2. Merge all others INTO the primary via HubSpot's merge endpoint
  3. HubSpot preserves the primary's IDs + merges associations from secondaries

Usage:
    cd /Users/gavincoleman/Downloads/email-scraper
    python3 scripts/dedupe_hubspot_companies.py            # dry run, shows what would merge
    DEDUPE_APPLY=1 python3 scripts/dedupe_hubspot_companies.py   # actually merge

Safe to re-run — merged companies disappear from the duplicate groups
automatically.
"""

from __future__ import annotations
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import re
from urllib.parse import urlparse

import requests
from hubspot import HubSpot

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("dedupe")


# Fields used to score which duplicate copy is the "primary" — more = better
PRIORITY_FIELDS = [
    "domain", "name", "city", "state", "description", "website",
    "business_vertical", "of_locations", "source__list_batch",
    "audit_url",
]


def derive_domain(props: dict) -> str:
    """Get domain from props['domain']; fall back to parsing props['website']."""
    d = (props.get("domain") or "").strip().lower()
    if d:
        return d.replace("www.", "").rstrip("/")
    website = props.get("website") or ""
    if not website:
        return ""
    parsed = urlparse(website if "://" in website else f"https://{website}")
    host = (parsed.netloc or parsed.path).lower()
    host = re.sub(r"^www\.", "", host)
    # Drop path, query, anchor
    host = host.split("/")[0].split("?")[0].split("#")[0]
    return host


def score_company(props: dict) -> int:
    """Higher score = more populated record. Used to pick which copy to keep."""
    return sum(1 for f in PRIORITY_FIELDS if props.get(f) and str(props[f]).strip())


def fetch_all_companies(h: HubSpot) -> list[dict]:
    """Paginate through all companies with the fields we care about."""
    all_results = []
    after = None
    while True:
        kwargs = {"limit": 100, "properties": PRIORITY_FIELDS}
        if after:
            kwargs["after"] = after
        page = h.crm.companies.basic_api.get_page(**kwargs)
        all_results.extend(page.results)
        if not page.paging or not page.paging.next:
            break
        after = page.paging.next.after
    return all_results


def main() -> int:
    if "HUBSPOT_ACCESS_TOKEN" not in os.environ:
        log.error("HUBSPOT_ACCESS_TOKEN not set")
        return 1

    apply_changes = os.environ.get("DEDUPE_APPLY") == "1"
    if not apply_changes:
        log.info("DRY RUN — set DEDUPE_APPLY=1 to actually merge")

    h = HubSpot(access_token=os.environ["HUBSPOT_ACCESS_TOKEN"])

    log.info("Fetching all companies (paginated)...")
    companies = fetch_all_companies(h)
    log.info("Got %d companies", len(companies))

    # Group by domain (deriving from website if domain field is empty)
    by_domain = defaultdict(list)
    no_domain = 0
    for c in companies:
        d = derive_domain(c.properties)
        if not d:
            # Last resort: dedupe by name (lowercased, stripped)
            name = (c.properties.get("name") or "").strip().lower()
            if name:
                d = f"name::{name}"
            else:
                no_domain += 1
                continue
        by_domain[d].append(c)

    log.info("Companies grouped: %d unique keys, %d skipped (no domain/name)",
             len(by_domain), no_domain)

    duplicate_groups = {d: cs for d, cs in by_domain.items() if len(cs) > 1}
    log.info("Domains with duplicates: %d", len(duplicate_groups))
    log.info("Total duplicate companies (excluding primaries): %d",
             sum(len(cs) - 1 for cs in duplicate_groups.values()))

    if not duplicate_groups:
        log.info("Nothing to dedupe.")
        return 0

    # For each duplicate group, pick primary (highest score) and merge others
    merged = 0
    errored = 0
    for i, (domain, copies) in enumerate(sorted(duplicate_groups.items()), 1):
        # Pick the most-populated copy as primary
        scored = sorted(copies, key=lambda c: (score_company(c.properties), -int(c.id)))
        primary = scored[-1]  # highest score
        to_merge = scored[:-1]  # everyone else

        log.info("[%d/%d] %s — %d copies; keeping id=%s (score=%d), merging %d others",
                 i, len(duplicate_groups), domain,
                 len(copies), primary.id, score_company(primary.properties), len(to_merge))

        if not apply_changes:
            for c in to_merge:
                log.info("    would merge: id=%s (score=%d) → into id=%s",
                         c.id, score_company(c.properties), primary.id)
            continue

        for c in to_merge:
            try:
                resp = requests.post(
                    "https://api.hubapi.com/crm/v3/objects/companies/merge",
                    headers={
                        "Authorization": f"Bearer {os.environ['HUBSPOT_ACCESS_TOKEN']}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "primaryObjectId": primary.id,
                        "objectIdToMerge": c.id,
                    },
                    timeout=15,
                )
                if resp.status_code in (200, 201):
                    merged += 1
                else:
                    errored += 1
                    log.warning("    merge failed for id=%s: %s %s",
                                c.id, resp.status_code, resp.text[:140])
            except Exception as exc:
                errored += 1
                log.warning("    merge failed for id=%s: %s", c.id, str(exc)[:120])

    log.info("=" * 60)
    if apply_changes:
        log.info("Done. Merged %d / Errors %d", merged, errored)
    else:
        log.info("Dry run complete — %d duplicate companies would be merged",
                 sum(len(cs) - 1 for cs in duplicate_groups.values()))
        log.info("Re-run with DEDUPE_APPLY=1 to actually merge")
    return 0


if __name__ == "__main__":
    sys.exit(main())
