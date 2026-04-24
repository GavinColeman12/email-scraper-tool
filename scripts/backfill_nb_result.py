#!/usr/bin/env python3
"""
One-time backfill: populate businesses.neverbounce_result from the
NB verdict embedded in email_source text.

Why: every existing row has NULL in the neverbounce_result column
because the scraper only wrote the verdict into email_source
("— NeverBounce VALID") for years. The learned_priors module reads
both columns with a fallback, but direct SQL queries and future
features will be faster + simpler if the dedicated column is
populated.

Idempotent: rows where neverbounce_result is already set get
SKIPPED. Run as many times as you like.

Usage:
    python scripts/backfill_nb_result.py             # dry-run
    python scripts/backfill_nb_result.py --commit    # persist
"""
import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.storage import _connect, _cursor, _PARAM


_NB_VERDICT_RE = re.compile(
    r"NeverBounce\s+(VALID|CATCH-ALL|UNKNOWN|INVALID)",
    re.IGNORECASE,
)


def _parse_nb(email_source: str) -> str:
    """Return the NB verdict as a lowercased canonical string, or ""
    when no verdict is embedded."""
    if not email_source:
        return ""
    m = _NB_VERDICT_RE.search(email_source)
    if not m:
        return ""
    raw = m.group(1).lower()
    return "catchall" if "catch" in raw else raw


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--commit", action="store_true",
                     help="Actually persist updates (default: dry-run)")
    args = ap.parse_args()

    conn = _connect()
    cur = _cursor(conn)

    # Pull every row where neverbounce_result is missing but
    # email_source has content to parse.
    cur.execute("""
        SELECT id, email_source, primary_email
          FROM businesses
         WHERE (neverbounce_result IS NULL OR neverbounce_result = '')
           AND email_source IS NOT NULL AND email_source <> ''
    """)
    rows = cur.fetchall()
    print(f"Scanning {len(rows)} rows with NULL neverbounce_result…",
          file=sys.stderr)

    counts = {"valid": 0, "catchall": 0, "unknown": 0, "invalid": 0,
               "no_verdict": 0}
    to_update: list[tuple[int, str]] = []

    for r in rows:
        d = dict(r) if hasattr(r, "keys") else dict(r)
        verdict = _parse_nb(d.get("email_source") or "")
        if not verdict:
            counts["no_verdict"] += 1
            continue
        counts[verdict] = counts.get(verdict, 0) + 1
        to_update.append((int(d["id"]), verdict))

    print("", file=sys.stderr)
    print(f"Would update {len(to_update)} rows:", file=sys.stderr)
    for k in ("valid", "catchall", "unknown", "invalid"):
        print(f"  {counts[k]:>5} → {k}", file=sys.stderr)
    print(f"  {counts['no_verdict']:>5} → no verdict in email_source "
          "(skipped)", file=sys.stderr)

    if not args.commit:
        print("\nDRY RUN — pass --commit to persist.", file=sys.stderr)
        conn.close()
        return 0

    # Batch update for speed — one executemany-style loop
    updated = 0
    for biz_id, verdict in to_update:
        cur.execute(
            f"UPDATE businesses SET neverbounce_result = {_PARAM} "
            f"WHERE id = {_PARAM}",
            (verdict, biz_id),
        )
        updated += 1
    conn.commit()
    conn.close()

    print(f"\n✅ Updated {updated} rows.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
