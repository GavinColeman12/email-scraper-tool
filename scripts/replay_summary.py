#!/usr/bin/env python3
"""
Print a pre-fix vs post-fix A/B summary across all replays.

Pairs up replays that share `original_search_id` and show baseline/post labels,
then prints side-by-side metrics + per-business diff counts.
"""
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.replay_storage import list_replays, get_replay


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--pre",  default="baseline-pre-L1-L8",
                    help="Label of the 'before' replay")
    ap.add_argument("--post", default="post-L1-L9",
                    help="Label of the 'after' replay")
    args = ap.parse_args()

    replays = list_replays()
    # Group by original_search_id, pick latest per label
    by_search = defaultdict(dict)
    for r in replays:
        label = r.get("label", "?")
        # Keep latest only (list_replays is ordered DESC by created_at)
        if label not in by_search[r["original_search_id"]]:
            by_search[r["original_search_id"]][label] = r

    print("=" * 78)
    print(f"A/B REPLAY SUMMARY — {args.pre} vs {args.post}")
    print("=" * 78)
    rows = []
    for sid in sorted(by_search.keys()):
        labels = by_search[sid]
        pre = labels.get(args.pre)
        post = labels.get(args.post)
        if not (pre and post):
            print(f"search #{sid}: missing pair ({list(labels.keys())})")
            continue
        pre_full = get_replay(pre["id"])
        post_full = get_replay(post["id"])
        ma = (pre_full.get("metrics") or {}).get("replay", {})
        mb = (post_full.get("metrics") or {}).get("replay", {})

        # Per-business change count
        am = {r["replay"]["business_id"]: r for r in (pre_full.get("businesses") or [])}
        bm = {r["replay"]["business_id"]: r for r in (post_full.get("businesses") or [])}
        changed = sum(1 for k, bb in bm.items()
                      if am.get(k) and am[k]["replay"].get("best_email") != bb["replay"].get("best_email"))
        total = min(len(am), len(bm))

        print(f"\n─── search #{sid} (n={ma.get('n')}) — {changed}/{total} emails changed ───")
        for k in ("has_email_pct", "safe_to_send_pct", "dm_email_pct",
                 "generic_email_pct", "pattern_detected_pct",
                 "nb_valid_pct", "risky_catchall_pct"):
            va, vb = ma.get(k, 0), mb.get(k, 0)
            d = round(vb - va, 1)
            arrow = "↑" if d > 0 else "↓" if d < 0 else "·"
            direction = "📈" if (
                (d > 0 and k in ("safe_to_send_pct", "dm_email_pct",
                                  "pattern_detected_pct", "nb_valid_pct"))
                or (d < 0 and k in ("generic_email_pct", "risky_catchall_pct"))
            ) else ("📉" if d != 0 else "·")
            print(f"  {k:24} {va:>7}% → {vb:>7}%  {arrow} {d:+.1f}  {direction}")

        rows.append({"sid": sid, "n": ma.get("n"), "changed": changed,
                     "safe_d": round(mb.get("safe_to_send_pct", 0) - ma.get("safe_to_send_pct", 0), 1),
                     "dm_d": round(mb.get("dm_email_pct", 0) - ma.get("dm_email_pct", 0), 1),
                     "generic_d": round(mb.get("generic_email_pct", 0) - ma.get("generic_email_pct", 0), 1)})

    if rows:
        print("\n" + "=" * 78)
        print("AGGREGATE")
        print("=" * 78)
        n_total = sum(r["n"] for r in rows)
        changed_total = sum(r["changed"] for r in rows)
        print(f"  searches compared: {len(rows)}")
        print(f"  total businesses: {n_total}")
        print(f"  emails changed: {changed_total} ({round(100*changed_total/n_total, 1)}%)")
        print(f"  safe_to_send: {sum(r['safe_d']*r['n'] for r in rows)/n_total:+.1f}%  weighted avg delta")
        print(f"  dm_email:     {sum(r['dm_d']*r['n'] for r in rows)/n_total:+.1f}%  weighted avg delta")
        print(f"  generic:      {sum(r['generic_d']*r['n'] for r in rows)/n_total:+.1f}%  weighted avg delta")


if __name__ == "__main__":
    main()
