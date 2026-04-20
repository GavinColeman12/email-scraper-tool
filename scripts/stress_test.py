#!/usr/bin/env python3
"""
Stress-test the triangulation pipeline on real businesses outside the
dental/legal/construction verticals already in the DB. Lets us catch
industry-specific regressions before they ship.

Each entry is a real, live business. Validate the pipeline produces:
  - a plausible decision maker (not junk, not cross-contaminated)
  - an email that's either triangulated+NB-valid OR below threshold (LOW)
  - no template-placeholder emails slipping through

Usage:
    python scripts/stress_test.py
    python scripts/stress_test.py --only "Salesforce"
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.universal_pipeline import scrape_with_triangulation


# Diverse real businesses — mixed industries, mixed sizes.
TEST_BUSINESSES = [
    # Plumbing (trades)
    {
        "business_name": "Roto-Rooter Plumbing & Water Cleanup",
        "business_type": "plumber",
        "website": "https://www.rotorooter.com/",
        "address": "255 E 5th St, Cincinnati, OH 45202",
    },
    # Restaurant (hospitality)
    {
        "business_name": "Franklin Barbecue",
        "business_type": "restaurant",
        "website": "https://franklinbbq.com/",
        "address": "900 E 11th St, Austin, TX 78702",
    },
    # Accounting / consulting (B2B services)
    {
        "business_name": "BKD CPAs & Advisors",
        "business_type": "accounting firm",
        "website": "https://www.forvis.com/",
        "address": "910 E St Louis St, Springfield, MO 65806",
    },
    # HVAC (trades)
    {
        "business_name": "Service Experts Heating & Air",
        "business_type": "hvac contractor",
        "website": "https://www.serviceexperts.com/",
        "address": "111 Great Neck Rd, Great Neck, NY 11021",
    },
    # SaaS startup (tech)
    {
        "business_name": "Notion Labs Inc",
        "business_type": "software company",
        "website": "https://www.notion.so/",
        "address": "548 Market St, San Francisco, CA 94104",
    },
    # Chiropractor (healthcare, non-dental)
    {
        "business_name": "The Joint Chiropractic",
        "business_type": "chiropractor",
        "website": "https://www.thejoint.com/",
        "address": "16767 N Perimeter Dr, Scottsdale, AZ 85260",
    },
    # Veterinarian (healthcare, non-human)
    {
        "business_name": "Banfield Pet Hospital",
        "business_type": "veterinarian",
        "website": "https://www.banfield.com/",
        "address": "8000 NE Tillamook St, Portland, OR 97213",
    },
    # Real estate (services)
    {
        "business_name": "The Corcoran Group",
        "business_type": "real estate agency",
        "website": "https://www.corcoran.com/",
        "address": "590 Madison Ave, New York, NY 10022",
    },
]


def audit_result(biz: dict, result) -> dict:
    """Apply plausibility checks and flag issues."""
    issues = []
    warnings = []
    positives = []

    best = result.best_email
    dm = result.decision_maker
    pat = result.detected_pattern

    if dm:
        positives.append(f"DM: {dm.full_name}")
    if pat:
        positives.append(f"pattern={pat.pattern_name} conf={pat.confidence}")
    if best:
        positives.append(f"email={best}")
        # Check for placeholder leakage
        PLACEHOLDER = {"first", "last", "firstname", "lastname", "your",
                       "youremail", "user", "example", "name"}
        local = best.split("@", 1)[0].lower() if "@" in best else ""
        if local in PLACEHOLDER:
            issues.append(f"placeholder email leaked: {best}")
        # Check for business-name-as-local-part (cross-contamination signal)
        if biz["business_name"].lower().replace(" ", "") in local:
            warnings.append(f"email local part matches business name — possible artifact: {best}")

    if not best:
        warnings.append("no email produced")

    if result.safe_to_send:
        positives.append("safe_to_send=True")
    elif best:
        warnings.append(
            f"safe_to_send=False (conf={result.best_email_confidence})"
        )

    if result.risky_catchall:
        warnings.append("catchall domain — delivery unverified")

    return {
        "business": biz["business_name"],
        "best_email": best,
        "dm": dm.full_name if dm else None,
        "confidence": result.best_email_confidence,
        "safe_to_send": result.safe_to_send,
        "pattern": pat.pattern_name if pat else None,
        "time_s": result.time_seconds,
        "agents_ok": result.agents_succeeded,
        "issues": issues,
        "warnings": warnings,
        "positives": positives,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--only", help="Substring match on business name to run just one")
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of human-readable")
    args = ap.parse_args()

    targets = TEST_BUSINESSES
    if args.only:
        targets = [b for b in TEST_BUSINESSES
                   if args.only.lower() in b["business_name"].lower()]

    results = []
    t0 = time.time()
    for i, biz in enumerate(targets):
        print(f"\n[{i+1}/{len(targets)}] {biz['business_name']!r} "
              f"({biz['business_type']}, {biz['address']})",
              file=sys.stderr)
        try:
            r = scrape_with_triangulation(biz)
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            results.append({"business": biz["business_name"],
                           "error": str(e)})
            continue
        audit = audit_result(biz, r)
        results.append(audit)
        if not args.json:
            print(f"  → {audit['best_email'] or '—'}  "
                  f"(DM: {audit['dm'] or '—'}, safe={audit['safe_to_send']}, "
                  f"t={audit['time_s']}s)")
            for p in audit["positives"]:
                print(f"    ✓ {p}")
            for w in audit["warnings"]:
                print(f"    ⚠ {w}")
            for i_ in audit["issues"]:
                print(f"    ❌ {i_}")

    total_t = round(time.time() - t0, 1)
    print(f"\n{'=' * 60}", file=sys.stderr)
    print(f"Audit summary ({total_t}s):", file=sys.stderr)
    print(f"  n={len(results)}", file=sys.stderr)
    n_email = sum(1 for r in results if r.get("best_email"))
    n_safe = sum(1 for r in results if r.get("safe_to_send"))
    n_issues = sum(len(r.get("issues") or []) for r in results)
    n_warnings = sum(len(r.get("warnings") or []) for r in results)
    print(f"  with email: {n_email}/{len(results)}", file=sys.stderr)
    print(f"  safe_to_send: {n_safe}/{len(results)}", file=sys.stderr)
    print(f"  hard issues: {n_issues}", file=sys.stderr)
    print(f"  soft warnings: {n_warnings}", file=sys.stderr)

    if args.json:
        print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
