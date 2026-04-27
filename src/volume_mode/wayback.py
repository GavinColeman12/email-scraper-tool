"""
Wayback Machine integration — free archive lookups via the CDX API.

Team pages get scrubbed when people leave. Old snapshots often still
list the previous DM (useful for triangulation even if they've moved
on, because the pattern they used — e.g. first.last@ — usually
stays the same across staff changes).

Uses web.archive.org's CDX search API to find snapshots of
/team /about /staff /our-people, then fetches up to 3 recent ones
per page and runs the same extraction we use on live pages.

Free, but respectfully rate-limited (0.5s between CDX calls).
"""
from __future__ import annotations

import logging
import time
from typing import Optional

import requests


logger = logging.getLogger(__name__)

CDX_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_PREFIX = "https://web.archive.org/web/{ts}/{url}"

# Pages worth archiving — same set as the live crawler's highest-value paths
WAYBACK_TARGET_PATHS = (
    "/about", "/about-us", "/team", "/our-team", "/staff",
    "/our-people", "/people", "/leadership", "/attorneys",
    "/our-attorneys", "/lawyers", "/doctors", "/our-doctors",
    "/providers", "/specialists", "/physicians",
)

# Respect web.archive.org by limiting fetches per domain
MAX_CDX_CALLS_PER_DOMAIN = 8
MAX_SNAPSHOTS_PER_PATH = 3
CDX_SLEEP_SECONDS = 0.4


def _cdx_query(domain: str, path: str, limit: int = 3,
               from_year: Optional[int] = None,
               to_year: Optional[int] = None) -> list[tuple[str, str]]:
    """
    Query the CDX API for the N most recent snapshots of {domain}{path}.
    Returns [(timestamp, original_url), ...]. Empty list on failure.

    If from_year/to_year are provided, bound the search window — useful
    for fetching historical snapshots (e.g. "team page as of 2019")
    which often have founder bios the current site has scrubbed.
    """
    target = f"{domain.rstrip('/')}{path}"
    params = {
        "url": target,
        "output": "json",
        "limit": f"-{limit}",    # most-recent-first
        "filter": "statuscode:200",
        "collapse": "timestamp:8",  # one per day
    }
    if from_year:
        params["from"] = f"{from_year}0101"
    if to_year:
        params["to"] = f"{to_year}1231"
    try:
        r = requests.get(CDX_URL, params=params, timeout=6)
        if r.status_code != 200:
            return []
        data = r.json()
    except Exception as e:
        logger.debug(f"wayback CDX {target} error: {e}")
        return []
    # First row is column headers; skip it.
    out: list[tuple[str, str]] = []
    for row in data[1:]:
        if len(row) >= 3:
            # row format: [urlkey, timestamp, original, mimetype, ...]
            out.append((row[1], row[2]))
    return out


def fetch_wayback_pages(
    domain: str, *, max_snapshots: int = 10, deadline_s: float = 20.0,
    historical_years: Optional[list[int]] = None,
) -> list[tuple[str, str]]:
    """
    Fetch Wayback snapshots of team/about pages for a domain.

    Returns [(url, html), ...]. Caller processes them the same as live
    pages — name extraction, email regex, etc.

    By default fetches the most-recent snapshots. Pass
    `historical_years=[2024, 2021, 2018]` to ALSO grab one snapshot per
    historical year — corporate redesigns scrub founder bios, and the
    2018 version often still has "Founded by Jane Smith in 2010" where
    the 2024 site dropped it.

    Free. Rate-limited to be polite. Respects a wall-clock deadline so
    it never blocks the caller for more than ~20s regardless of how
    many snapshots are available.
    """
    if not domain:
        return []
    domain_stripped = domain.lower().replace("https://", "").replace("http://", "")
    domain_stripped = domain_stripped.rstrip("/").split("/")[0]

    out: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    t_start = time.time()
    cdx_calls = 0

    # Build the list of (year_range_or_None, limit_per_path) queries:
    # None = most-recent; (year, year) = one snapshot in that year.
    year_queries: list[tuple[Optional[tuple[int, int]], int]] = [
        (None, MAX_SNAPSHOTS_PER_PATH),  # recent
    ]
    if historical_years:
        for y in historical_years:
            year_queries.append(((y, y), 1))

    for path in WAYBACK_TARGET_PATHS:
        if len(out) >= max_snapshots:
            break
        if cdx_calls >= MAX_CDX_CALLS_PER_DOMAIN:
            break
        if time.time() - t_start > deadline_s:
            break

        for yr_range, per_path_limit in year_queries:
            if len(out) >= max_snapshots:
                break
            if cdx_calls >= MAX_CDX_CALLS_PER_DOMAIN:
                break
            if time.time() - t_start > deadline_s:
                break
            fy, ty = (yr_range if yr_range else (None, None))
            snaps = _cdx_query(domain_stripped, path,
                                limit=per_path_limit,
                                from_year=fy, to_year=ty)
            cdx_calls += 1
            time.sleep(CDX_SLEEP_SECONDS)
            for ts, original in snaps:
                if len(out) >= max_snapshots:
                    break
                if time.time() - t_start > deadline_s:
                    break
                snap_url = WAYBACK_PREFIX.format(ts=ts, url=original)
                if snap_url in seen_urls:
                    continue
                seen_urls.add(snap_url)
                try:
                    r = requests.get(snap_url, timeout=7, headers={
                        "User-Agent": "Mozilla/5.0 (compatible; volume-mode/1.0)",
                    })
                    if r.status_code == 200 and "text/html" in r.headers.get("content-type", ""):
                        out.append((snap_url, r.text))
                except Exception:
                    continue
                time.sleep(CDX_SLEEP_SECONDS)
    return out
