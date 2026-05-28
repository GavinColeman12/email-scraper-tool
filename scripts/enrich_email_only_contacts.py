#!/usr/bin/env python3
"""Enrich HubSpot contacts that have only an email (no company/name/vertical).

For each target email:
  1. Parse person name from the email local-part (adam.dayan@ -> Adam Dayan)
  2. Fetch the domain's homepage, extract company name from <title>
  3. Classify the business vertical from name + domain keywords
  4. Update the HubSpot contact (first/last name) + upsert company
     (name, domain, vertical) + associate + create a deal at Cold Outreach

Input: a JSON list of emails (default: exports/april_deliverable_emails.json,
filtered to those NOT already enriched). Override with EMAILS_FILE env var.

Usage:
    cd /Users/gavincoleman/Downloads/email-scraper
    python3 scripts/enrich_email_only_contacts.py            # dry run
    DRY_RUN=0 python3 scripts/enrich_email_only_contacts.py  # apply
"""

from __future__ import annotations
import json, os, re, sys, time
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests
import psycopg2
from src.hubspot_sync import sync_lead_to_hubspot, _classify_vertical

_ACRO = {"pa","llc","llp","pllc","pc","dds","md","dmd","cpa","inc","co","ltd","esq"}
_SMALL = {"of","the","and","for","a","an","to","in","on","at","by","&","or"}


def _smart_title_case(name: str) -> str:
    if not name or any(c.islower() for c in name):
        return name
    out = []
    for i, w in enumerate(name.split()):
        bare = w.lower().rstrip(".,").replace(".", "")
        if bare in _ACRO:
            out.append(w.upper())
        elif w.lower().rstrip(".,") in _SMALL and i != 0:
            out.append(w.lower())
        else:
            out.append(w.capitalize())
    return " ".join(out)

DRY_RUN = os.environ.get("DRY_RUN", "1") == "1"
EMAILS_FILE = os.environ.get("EMAILS_FILE", "exports/april_deliverable_emails.json")

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# Free-email providers — can't derive a company, skip enrichment
FREE_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "aol.com", "icloud.com", "me.com", "mac.com", "msn.com", "live.com",
    "proton.me", "protonmail.com", "gmx.com", "ymail.com", "comcast.net",
}
# Obvious non-leads (Gavin's own addresses, junk)
SKIP_LOCALPARTS_CONTAINING = {"gavin", "test", "noreply", "no-reply"}


def parse_name(email: str) -> tuple[str, str]:
    """Best-effort first/last from email local-part."""
    local = email.split("@")[0]
    local = re.sub(r"\d+", "", local)  # drop digits
    if "." in local:
        parts = local.split(".")
        if len(parts) >= 2 and len(parts[0]) > 1 and len(parts[1]) > 1:
            return parts[0].capitalize(), parts[1].capitalize()
    # initial+last heuristic: "jsmith" -> J Smith is unreliable; leave last only
    return "", ""


def company_from_site(domain: str) -> str:
    """Fetch homepage, extract a clean company name from <title>."""
    for scheme in ("https://", "http://"):
        try:
            r = requests.get(scheme + domain, headers=UA, timeout=8, allow_redirects=True)
            if r.status_code < 400 and r.text:
                m = re.search(r"<title[^>]*>(.*?)</title>", r.text, re.I | re.S)
                if m:
                    title = re.sub(r"\s+", " ", m.group(1)).strip()
                    # Take the part before a separator (| - – —  :)
                    name = re.split(r"\s*[|\-–—:]\s*", title)[0].strip()
                    # Drop generic prefixes
                    if name and len(name) < 60 and name.lower() not in ("home", "welcome"):
                        return name
            break
        except Exception:
            continue
    # Fallback: derive from domain
    base = domain.split(".")[0]
    return _smart_title_case(base.replace("-", " ").upper())


def main() -> int:
    TOKEN = os.environ["HUBSPOT_ACCESS_TOKEN"]
    HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

    emails = set(json.loads(Path(EMAILS_FILE).read_text()))
    emails = {e for e in emails if "@bcc." not in e and "crescendo-consulting" not in e}

    # Only enrich ones NOT in Neon (email-only)
    conn = psycopg2.connect(os.environ["DATABASE_URL"]); cur = conn.cursor()
    cur.execute("SELECT LOWER(primary_email) FROM businesses WHERE primary_email IS NOT NULL")
    neon = {r[0] for r in cur.fetchall()}; conn.close()
    raw_targets = sorted(emails - neon)
    # Filter out free-email domains + junk
    targets = []
    skipped = []
    for e in raw_targets:
        local, _, dom = e.partition("@")
        if dom in FREE_EMAIL_DOMAINS:
            skipped.append((e, "free-email domain")); continue
        if any(s in local.lower() for s in SKIP_LOCALPARTS_CONTAINING):
            skipped.append((e, "skip-localpart")); continue
        targets.append(e)
    print(f"Email-only: {len(raw_targets)} total → {len(targets)} enrichable, {len(skipped)} skipped\n")
    if skipped:
        print("Skipped (free-email/junk — can't enrich to a company):")
        for e, why in skipped:
            print(f"  - {e}  ({why})")
        print()

    enriched = errored = 0
    for i, email in enumerate(targets, 1):
        domain = email.split("@")[1]
        first, last = parse_name(email)
        company = company_from_site(domain)
        vertical = _classify_vertical(company)  # classify from company name text
        print(f"[{i}/{len(targets)}] {email}")
        print(f"    name={first or '?'} {last or '?'}  company={company!r}  vertical={vertical}")

        if DRY_RUN:
            continue

        # Build a business dict and sync (creates company + updates contact + deal)
        biz = {
            "business_name": company,
            "website": f"https://{domain}",
            "primary_email": email,
            "contact_name": f"{first} {last}".strip(),
            "contact_title": "",
            "industry": vertical,
            "source_batch": "april-deliverable-enriched",
        }
        try:
            res = sync_lead_to_hubspot(business=biz, lead_score_0_100=60)
            if res.error:
                errored += 1; print(f"    ERROR: {res.error}")
            else:
                enriched += 1
        except Exception as e:
            errored += 1; print(f"    ERROR: {e}")
        time.sleep(0.4)

    print(f"\n{'DRY RUN — nothing changed' if DRY_RUN else f'Enriched {enriched}, errored {errored}'}")
    if DRY_RUN:
        print("Re-run with DRY_RUN=0 to apply.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
