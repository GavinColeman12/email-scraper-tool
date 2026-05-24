#!/usr/bin/env python3
"""London batch expansion — runs the 5 verticals that failed in the original
batch (DNS blip) plus 5 high-yield UK additions.

Verticals chosen for high decision-maker exposure:
  - Boutique law firms / solicitors → founders are visible
  - Chartered accountants → senior partners are findable
  - Marketing agencies / consultancies → founders blog publicly
  - Private dental / michelin / private members clubs → owner-led

Each vertical creates a new search_id (no dedup with original batch).
Place-id dedup at the businesses table prevents true duplicates.
"""

from __future__ import annotations
import sys
from pathlib import Path

# Reuse the main batch script — only override VERTICALS
sys.path.insert(0, str(Path(__file__).resolve().parent))
import london_batch_scrape  # noqa

london_batch_scrape.VERTICALS = [
    # Failed-retry from original batch (DNS blip at 10:09:54)
    "solicitor",
    "chartered accountant",
    "marketing agency",
    "management consultant",
    "boutique consultancy",
    # High-yield UK additions
    "private dentist",
    "michelin star restaurant",
    "private members club",
    "independent hotel",
    "boutique law firm",
]

if __name__ == "__main__":
    sys.exit(london_batch_scrape.main())
