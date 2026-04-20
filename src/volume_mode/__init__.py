"""
Volume Mode — cheap mass email discovery.

Target: <$0.025/biz, 25-40% verified + 30-50% guess hit rate.
Primary entry point: scrape_volume(business) from pipeline.py.

Philosophy:
  - Time is free, money isn't. Crawl exhaustively; verify cheaply;
    guess conservatively.
  - Decision makers first; industry prior is the LAST resort.
  - Generic inboxes (info@, contact@, smile@, hello@, …) are NEVER
    picked as the primary email. Ever.

See docs/superpowers/specs/2026-04-20-volume-mode-design.md.
"""
from src.volume_mode.pipeline import scrape_volume, VolumeResult  # noqa: F401
