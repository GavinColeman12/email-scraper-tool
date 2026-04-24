"""
Learn pattern priorities per vertical from our own send-safe history.

Replaces hardcoded "{first}.{last} is the top pattern for law firms"
heuristics with EMPIRICAL data from every NB-valid send we've ever
produced. As the scraper runs more campaigns, the learned priors get
more accurate — a genuine feedback loop.

Process:
  1. Query every business where NB returned "valid" and we have
     first_name + last_name + primary_email populated
  2. For each row, classify which pattern produced its email:
       {first}.{last}   paula.wyatt@firm.com
       {f}{last}        pwyatt@firm.com
       {first}{l}       paulaw@firm.com
       {first}           paula@firm.com
       {last}            wyatt@firm.com
       dr{last}         drwyatt@firm.com
       dr.{last}        dr.wyatt@firm.com
       ... etc
  3. Group by vertical (via normalize_vertical) and count
  4. Return sorted patterns per vertical

Result: rescue + future scrapes use the patterns that actually WORK
for each industry, learned from our ground truth.

Reads NB verdict from both:
  - neverbounce_result column (when populated)
  - email_source text fallback ("— NeverBounce VALID")

Cached at module level with a TTL (default 1 hour) so the learner
doesn't hit the DB on every rescue call within a bulk run.
"""
from __future__ import annotations

import re
import time
from typing import Optional


# ──────────────────────────────────────────────────────────────────────
# Pattern classifier — identify which pattern produced a given email
# ──────────────────────────────────────────────────────────────────────

def classify_pattern(email: str, first: str, last: str) -> Optional[str]:
    """
    Given a known email + DM first/last name, return the pattern name
    that produced the local part. Returns None if the email local
    doesn't match any standard pattern (shared inbox, different person,
    etc. — these aren't useful training data).
    """
    if not email or "@" not in email or not first or not last:
        return None
    local = email.split("@", 1)[0].lower().strip()
    f = first.lower().strip()
    l = last.lower().strip()
    if not f or not l:
        return None

    # Check the specific patterns in priority order (most-specific first
    # so "first.last" matches before "first" alone)
    if local == f"{f}.{l}":
        return "first.last"
    if local == f"{f[0]}{l}":
        return "flast"
    if local == f"{f}{l[0]}":
        return "firstl"
    if local == f"{f}_{l}":
        return "first_last"
    if local == f"{f}-{l}":
        return "first-last"
    if local == f"{l}.{f}":
        return "last.first"
    if local == f"{l}_{f}":
        return "last_first"
    if local == f"{l}{f}":
        return "lastfirst"
    if local == f"{f}.{l[0]}":
        return "first.l"
    if local == f"{f[0]}.{l}":
        return "f.last"
    if local == f"{f[0]}{l[0]}":
        return "fl"
    if local == f:
        return "first"
    if local == l:
        return "last"
    # Doctor / medical prefixes
    if local == f"dr{l}":
        return "drlast"
    if local == f"dr.{l}":
        return "dr.last"
    if local == f"dr{f}":
        return "drfirst"
    if local == f"dr.{f}":
        return "dr.first"
    if local == f"doctor{l}":
        return "doctorlast"
    if local == f"doctor.{l}":
        return "doctor.last"
    return None


# ──────────────────────────────────────────────────────────────────────
# NB verdict extraction — row's neverbounce_result OR email_source text
# ──────────────────────────────────────────────────────────────────────

_NB_VERDICT_RE = re.compile(r"NeverBounce\s+(VALID|CATCH-ALL|UNKNOWN|INVALID)",
                             re.IGNORECASE)


def _nb_verdict_of(row: dict) -> str:
    """Return the row's NB verdict, checking the dedicated column first
    and falling back to parsing email_source text ("— NeverBounce VALID")
    for rows scraped before neverbounce_result was being populated."""
    direct = (row.get("neverbounce_result") or "").lower().strip()
    if direct:
        return direct
    source = row.get("email_source") or ""
    m = _NB_VERDICT_RE.search(source)
    if not m:
        return ""
    raw = m.group(1).lower()
    # Normalize "catch-all" → "catchall" to match the direct-column format
    if "catch" in raw:
        return "catchall"
    return raw


def _first_last_of(row: dict) -> tuple[str, str]:
    """Pull first + last name from a row, tolerating both split columns
    and a single contact_name string. Strips "Dr." titles and common
    credentials (DDS, MD, JD)."""
    first = (row.get("first_name") or row.get("contact_first") or "").strip()
    last = (row.get("last_name") or row.get("contact_last") or "").strip()
    if not first and not last:
        full = (row.get("contact_name") or "").strip()
        # Strip title prefix
        full = re.sub(r"^(dr\.?\s+|doctor\s+|prof\.?\s+)",
                      "", full, flags=re.IGNORECASE)
        # Strip trailing credentials: ", DMD", " MD", "JD"
        full = re.sub(
            r"[,\s]+(DMD|DDS|MD|MS|PhD|JD|Esq|P\.?C\.?|LLP|LLC|PLLC|Inc)"
            r"[.,]?$", "", full, flags=re.IGNORECASE,
        )
        parts = full.split(None, 1)
        first = parts[0] if parts else ""
        last = parts[1] if len(parts) > 1 else ""
    return first, last


# ──────────────────────────────────────────────────────────────────────
# Aggregator — group by vertical, rank patterns by hit count
# ──────────────────────────────────────────────────────────────────────

# Minimum sample size for vertical-specific learned priors. Below
# this, we fall back to global priors; below the global threshold,
# we return an empty list and the caller uses hardcoded defaults.
MIN_SAMPLES_PER_VERTICAL = 5
MIN_SAMPLES_GLOBAL = 10

# Cache the learned distribution — refreshed every hour since the DB
# grows slowly and learning is expensive.
_CACHE: dict = {"priors": None, "computed_at": 0}
_CACHE_TTL_SECONDS = 3600


def compute_learned_priors(force_refresh: bool = False) -> dict:
    """
    Walk every NB-valid business row, classify its email pattern, and
    return the aggregate:
      {
        "global": [(pattern, count, pct), ...],  # sorted by count desc
        "by_vertical": {
            "Law / Legal": [(pattern, count, pct), ...],
            "Dental":      [...],
            ...
        },
        "total_samples": int,
      }

    Cached for 1 hour. Pass force_refresh=True to recompute.
    """
    if not force_refresh and _CACHE["priors"] is not None:
        if time.time() - _CACHE["computed_at"] < _CACHE_TTL_SECONDS:
            return _CACHE["priors"]

    try:
        from src.storage import _connect, _cursor
        from src.dashboard_queries import normalize_vertical
    except Exception:
        return {"global": [], "by_vertical": {}, "total_samples": 0}

    conn = _connect()
    try:
        cur = _cursor(conn)
        # Pull every row with an email + name. We classify in Python
        # because the pattern matching is richer than SQL.
        cur.execute("""
            SELECT primary_email, contact_name,
                   business_type, neverbounce_result, email_source,
                   cms, cms_provider_hint, cms_catchall_hint
              FROM businesses
             WHERE primary_email IS NOT NULL AND primary_email <> ''
               AND contact_name IS NOT NULL AND contact_name <> ''
        """)
        rows = cur.fetchall()
    except Exception:
        return {"global": [], "by_vertical": {}, "total_samples": 0}
    finally:
        conn.close()

    global_counts: dict[str, int] = {}
    vertical_counts: dict[str, dict[str, int]] = {}
    # CMS cross-tabs — track pattern distribution per platform so we
    # can surface "what's the top pattern for Wix vs Squarespace sites
    # in the Dental vertical?"
    cms_counts: dict[str, dict[str, int]] = {}
    # Also track NB verdict distribution per CMS — feeds the
    # "catchall on Wix = expected, catchall on Squarespace = suspect"
    # interpretation layer.
    cms_nb_counts: dict[str, dict[str, int]] = {}

    for raw in rows:
        row = dict(raw) if hasattr(raw, "keys") else dict(raw)
        verdict = _nb_verdict_of(row)
        cms = (row.get("cms") or "").lower().strip()
        if cms and verdict:
            cms_nb_counts.setdefault(cms, {})[verdict] = (
                cms_nb_counts.setdefault(cms, {}).get(verdict, 0) + 1
            )
        if verdict != "valid":
            continue
        first, last = _first_last_of(row)
        pattern = classify_pattern(
            row.get("primary_email") or "", first, last,
        )
        if not pattern:
            continue
        vertical = normalize_vertical(row.get("business_type") or "")
        global_counts[pattern] = global_counts.get(pattern, 0) + 1
        vc = vertical_counts.setdefault(vertical, {})
        vc[pattern] = vc.get(pattern, 0) + 1
        if cms:
            cc = cms_counts.setdefault(cms, {})
            cc[pattern] = cc.get(pattern, 0) + 1

    total = sum(global_counts.values())

    def _rank(counts: dict[str, int]) -> list:
        n = sum(counts.values()) or 1
        return sorted(
            [(p, c, round(100 * c / n, 1)) for p, c in counts.items()],
            key=lambda x: -x[1],
        )

    priors = {
        "global": _rank(global_counts),
        "by_vertical": {v: _rank(c) for v, c in vertical_counts.items()},
        "by_cms": {cms: _rank(c) for cms, c in cms_counts.items()},
        "cms_nb_distribution": {
            cms: {v: c for v, c in counts.items()}
            for cms, counts in cms_nb_counts.items()
        },
        "total_samples": total,
    }
    _CACHE["priors"] = priors
    _CACHE["computed_at"] = time.time()
    return priors


def top_patterns_for_vertical(
    vertical: str, top_n: int = 5,
) -> list[str]:
    """
    Return the top-N pattern names for a vertical, ranked by historical
    hit rate in our send-safe database. Falls back to global priors if
    the vertical has too few samples, and returns [] if the global set
    is also too small (caller uses hardcoded defaults).
    """
    priors = compute_learned_priors()
    # Try vertical-specific first
    per_vert = priors["by_vertical"].get(vertical) or []
    if sum(c for _, c, _ in per_vert) >= MIN_SAMPLES_PER_VERTICAL:
        return [p for p, _, _ in per_vert[:top_n]]
    # Fall back to global
    global_list = priors["global"]
    if sum(c for _, c, _ in global_list) >= MIN_SAMPLES_GLOBAL:
        return [p for p, _, _ in global_list[:top_n]]
    return []


def summarize_for_ui() -> dict:
    """Compact dict for the Pattern Learning page / rescue UI —
    shows total sample count + the per-vertical + per-CMS rankings
    in a readable shape."""
    priors = compute_learned_priors()
    return {
        "total_samples": priors["total_samples"],
        "global_top_3": priors["global"][:3],
        "verticals_with_data": {
            v: {
                "samples": sum(c for _, c, _ in rows),
                "top_3": rows[:3],
            }
            for v, rows in priors["by_vertical"].items()
            if sum(c for _, c, _ in rows) >= MIN_SAMPLES_PER_VERTICAL
        },
        "cms_with_data": {
            cms: {
                "samples": sum(c for _, c, _ in rows),
                "top_3": rows[:3],
                "nb_verdicts": priors["cms_nb_distribution"].get(cms, {}),
            }
            for cms, rows in priors["by_cms"].items()
            if sum(c for _, c, _ in rows) >= MIN_SAMPLES_PER_VERTICAL
        },
    }
