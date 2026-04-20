#!/usr/bin/env python3
"""
Replay triangulation against a historical search.

Usage:
    # Capture a replay with the current code
    python scripts/replay_search.py run --search-id 29 --label baseline

    # Diff two replays of the same search
    python scripts/replay_search.py diff <replay_id_before> <replay_id_after>

    # List replays
    python scripts/replay_search.py list [--search-id 29]

Near-zero cost because Phase 1-3 caches (owner_candidates 30d, domain_emails
14d, detected_pattern 60d, nb_verify 30d, smtp_probe 7d) already hold the
results of the original run. Only Phase 4-7 logic runs; it re-uses cached
discovery + verification data.
"""
import argparse
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path

# Allow "python scripts/replay_search.py" from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.storage import list_businesses, get_search
from src.replay_storage import save_replay, list_replays, get_replay
from src.universal_pipeline import scrape_with_triangulation


# Local parts that indicate a generic shared inbox (not a decision maker).
GENERIC_LOCAL_PARTS = {
    "info", "contact", "contactus", "hello", "hi", "team", "support",
    "admin", "office", "mail", "email", "enquiries", "inquiries",
    "sales", "marketing", "help", "service", "customercare", "reception",
    "smile", "appointments", "bookings",
}


def _is_generic_local(email: str) -> bool:
    if not email or "@" not in email:
        return False
    local = email.split("@", 1)[0].lower()
    return local in GENERIC_LOCAL_PARTS


def _is_dm_local(email: str, dm_first: str, dm_last: str) -> bool:
    """Heuristic: does the local part contain the DM's first or last name?"""
    if not email or "@" not in email or not (dm_first or dm_last):
        return False
    local = email.split("@", 1)[0].lower()
    f = (dm_first or "").lower().strip()
    l = (dm_last or "").lower().strip()
    if f and f in local:
        return True
    if l and l in local:
        return True
    # first-initial + last patterns
    if f and l and (f[0] + l) in local:
        return True
    return False


def _compute_metrics(rows: list) -> dict:
    n = len(rows) or 1
    safe = sum(1 for r in rows if r.get("safe_to_send"))
    generic = sum(1 for r in rows if _is_generic_local(r.get("best_email") or ""))
    dm_email = sum(
        1 for r in rows
        if _is_dm_local(
            r.get("best_email") or "",
            (r.get("decision_maker") or {}).get("first_name", ""),
            (r.get("decision_maker") or {}).get("last_name", ""),
        )
    )
    pattern_detected = sum(1 for r in rows if r.get("detected_pattern"))
    nb_valid = sum(1 for r in rows
                   if r.get("best_email_nb_result") == "valid")
    has_email = sum(1 for r in rows if r.get("best_email"))
    risky_catchall = sum(1 for r in rows if r.get("risky_catchall"))
    return {
        "n": len(rows),
        "has_email_pct": round(100 * has_email / n, 1),
        "safe_to_send_pct": round(100 * safe / n, 1),
        "dm_email_pct": round(100 * dm_email / n, 1),
        "generic_email_pct": round(100 * generic / n, 1),
        "pattern_detected_pct": round(100 * pattern_detected / n, 1),
        "nb_valid_pct": round(100 * nb_valid / n, 1),
        "risky_catchall_pct": round(100 * risky_catchall / n, 1),
    }


def _serialise_result(result) -> dict:
    """Extract the decision-relevant fields from a TriangulationResult."""
    dm = asdict(result.decision_maker) if result.decision_maker else None
    pat = asdict(result.detected_pattern) if result.detected_pattern else None
    top_nb = None
    for c in (result.candidate_emails or []):
        if c.get("email") == result.best_email:
            top_nb = c.get("nb_result")
            break
    return {
        "best_email": result.best_email,
        "best_email_confidence": result.best_email_confidence,
        "best_email_nb_result": top_nb,
        "safe_to_send": result.safe_to_send,
        "risky_catchall": result.risky_catchall,
        "decision_maker": dm,
        "detected_pattern": pat,
        "candidate_emails": result.candidate_emails,
        "agents_succeeded": result.agents_succeeded,
        "time_seconds": result.time_seconds,
    }


def _original_row(b: dict) -> dict:
    """Project a stored business row into the same shape as a replay result."""
    pat = None
    if b.get("triangulation_pattern"):
        pat = {
            "pattern_name": b.get("triangulation_pattern"),
            "confidence": b.get("triangulation_confidence") or 0,
            "method": b.get("triangulation_method") or "unknown",
        }
    dm = None
    cn = b.get("contact_name") or ""
    if cn:
        parts = cn.strip().split(None, 1)
        dm = {
            "first_name": parts[0] if parts else "",
            "last_name": parts[1] if len(parts) > 1 else "",
            "full_name": cn,
        }
    return {
        "best_email": b.get("primary_email"),
        "best_email_confidence": b.get("triangulation_confidence") or 0,
        "best_email_nb_result": b.get("neverbounce_result"),
        "safe_to_send": bool(b.get("email_safe_to_send")),
        "risky_catchall": False,  # not stored separately
        "decision_maker": dm,
        "detected_pattern": pat,
    }


def run_replay(search_id: int, label: str, limit: int = None,
               verbose: bool = True) -> int:
    search = get_search(search_id)
    if not search:
        print(f"ERROR: search {search_id} not found", file=sys.stderr)
        sys.exit(1)

    businesses = list_businesses(search_id=search_id)
    if limit:
        businesses = businesses[:limit]

    if verbose:
        print(f"Replaying {len(businesses)} businesses from search #{search_id} "
              f"({search.get('query')!r}) label={label!r}", file=sys.stderr)

    replay_rows = []
    original_rows = []
    t_start = time.time()
    for i, b in enumerate(businesses):
        if not b.get("website"):
            continue
        try:
            result = scrape_with_triangulation(b)
            row = _serialise_result(result)
        except Exception as e:
            if verbose:
                print(f"  [{i+1}/{len(businesses)}] {b.get('business_name')!r} ERROR: {e}",
                      file=sys.stderr)
            continue
        row["business_id"] = b.get("id")
        row["business_name"] = b.get("business_name")
        row["website"] = b.get("website")
        row["address"] = b.get("address")
        replay_rows.append(row)

        orig = _original_row(b)
        orig["business_id"] = b.get("id")
        orig["business_name"] = b.get("business_name")
        original_rows.append(orig)

        if verbose:
            delta_flag = "✱" if orig.get("best_email") != row["best_email"] else " "
            print(f"  [{i+1}/{len(businesses)}] {delta_flag} {b.get('business_name')[:40]:40} "
                  f"{orig.get('best_email') or '—':40} → {row['best_email'] or '—'}",
                  file=sys.stderr)

    t_total = round(time.time() - t_start, 1)
    metrics = {
        "elapsed_seconds": t_total,
        "baseline": _compute_metrics(original_rows),
        "replay": _compute_metrics(replay_rows),
    }
    metrics["deltas"] = {
        k: round(metrics["replay"][k] - metrics["baseline"][k], 1)
        for k in metrics["replay"].keys() if k != "n" and isinstance(metrics["replay"][k], (int, float))
    }

    combined = []
    for o, r in zip(original_rows, replay_rows):
        combined.append({"original": o, "replay": r,
                         "changed": o.get("best_email") != r.get("best_email")})

    replay_id = save_replay(search_id, label, combined, metrics)

    if verbose:
        print(f"\nReplay #{replay_id} saved in {t_total}s", file=sys.stderr)
        _print_metrics(metrics)

    return replay_id


def _print_metrics(metrics: dict) -> None:
    b = metrics["baseline"]
    r = metrics["replay"]
    d = metrics["deltas"]
    print("\n" + "=" * 60)
    print(f"  {'metric':24} {'baseline':>10} {'replay':>10} {'delta':>10}")
    print("-" * 60)
    for k in ("n", "has_email_pct", "safe_to_send_pct", "dm_email_pct",
             "generic_email_pct", "pattern_detected_pct", "nb_valid_pct",
             "risky_catchall_pct"):
        if k == "n":
            print(f"  {k:24} {b[k]:>10} {r[k]:>10}")
        else:
            delta = d.get(k, 0)
            arrow = "↑" if delta > 0 else "↓" if delta < 0 else "·"
            print(f"  {k:24} {b[k]:>10} {r[k]:>10} {arrow} {delta:+.1f}")
    print("=" * 60)


def cmd_diff(replay_id_a: int, replay_id_b: int) -> None:
    a = get_replay(replay_id_a)
    b = get_replay(replay_id_b)
    if not a or not b:
        print("ERROR: one or both replay IDs not found", file=sys.stderr)
        sys.exit(1)
    print(f"Replay A #{a['id']} label={a.get('label')!r} git={a.get('git_sha')}")
    print(f"Replay B #{b['id']} label={b.get('label')!r} git={b.get('git_sha')}")
    ma = a["metrics"]["replay"]
    mb = b["metrics"]["replay"]
    print("-" * 60)
    print(f"  {'metric':24} {'A':>10} {'B':>10} {'delta':>10}")
    for k in sorted(set(ma.keys()) | set(mb.keys())):
        va, vb = ma.get(k, 0), mb.get(k, 0)
        if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
            d = round(vb - va, 1) if k != "n" else vb - va
            arrow = "↑" if d > 0 else "↓" if d < 0 else "·"
            print(f"  {k:24} {va:>10} {vb:>10} {arrow} {d:+.1f}")
    # Per-business changes where best_email differs
    am = {r["replay"]["business_id"]: r for r in (a.get("businesses") or [])}
    bm = {r["replay"]["business_id"]: r for r in (b.get("businesses") or [])}
    changes = []
    for biz_id, bb in bm.items():
        aa = am.get(biz_id)
        if not aa:
            continue
        if aa["replay"].get("best_email") != bb["replay"].get("best_email"):
            changes.append((bb["replay"].get("business_name"),
                           aa["replay"].get("best_email"),
                           bb["replay"].get("best_email")))
    if changes:
        print(f"\nPer-business changes ({len(changes)}):")
        for name, ae, be in changes[:30]:
            print(f"  {name[:40]:40} {ae or '—':40} → {be or '—'}")


def cmd_list(search_id: int = None) -> None:
    rows = list_replays(search_id)
    if not rows:
        print("No replays yet.")
        return
    print(f"{'id':>4} {'search':>7} {'created':20} {'git':>8} label")
    for r in rows:
        print(f"{r['id']:>4} {r['original_search_id']:>7} {str(r['created_at'])[:19]:20} "
              f"{(r['git_sha'] or '')[:7]:>8} {r.get('label') or ''}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="Run a replay against a search")
    r.add_argument("--search-id", type=int, required=True)
    r.add_argument("--label", default="replay")
    r.add_argument("--limit", type=int, default=None)
    r.add_argument("--quiet", action="store_true")

    d = sub.add_parser("diff", help="Diff two replays")
    d.add_argument("replay_a", type=int)
    d.add_argument("replay_b", type=int)

    l = sub.add_parser("list", help="List replays")
    l.add_argument("--search-id", type=int, default=None)

    args = ap.parse_args()

    if args.cmd == "run":
        run_replay(args.search_id, args.label, args.limit, not args.quiet)
    elif args.cmd == "diff":
        cmd_diff(args.replay_a, args.replay_b)
    elif args.cmd == "list":
        cmd_list(args.search_id)


if __name__ == "__main__":
    main()
