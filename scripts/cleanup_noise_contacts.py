#!/usr/bin/env python3
"""Remove contacts from HubSpot that aren't part of the scraped lead pool.

When Gavin's Gmail was connected to HubSpot, the default settings auto-created
contacts for every email recipient — including Stripe receipts, DocuSign
system emails, Publicis Sapient colleagues, random Gmail correspondence.
This pollutes the CRM with non-lead contacts.

This script:
  1. Pulls all contacts in HubSpot
  2. Pulls all primary_emails in Neon's businesses table
  3. Anything in HubSpot whose email is NOT in Neon = noise → delete
  4. ALWAYS spares: Gavin, Joanna, any contact explicitly whitelisted

Always dry-runs by default. Set CLEANUP_APPLY=1 to actually delete.

Usage:
    cd /Users/gavincoleman/Downloads/email-scraper
    python3 scripts/cleanup_noise_contacts.py              # dry run
    CLEANUP_APPLY=1 python3 scripts/cleanup_noise_contacts.py  # actually delete

Safety: HubSpot supports contact restore from the recycle bin within 90 days.
Mistakes are recoverable.
"""

from __future__ import annotations
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import psycopg2
import requests

TOKEN = os.environ['HUBSPOT_ACCESS_TOKEN']
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Whitelist — never delete these
WHITELIST_EMAILS = {
    "gavincol@bu.edu",
    "gavin.coleman@publicissapient.com",  # Gavin's work email
    "jo.webber@crescendo-consulting.net",  # Joanna
    "joanna.webber@crescendo-consulting.net",  # alt spelling
}
WHITELIST_DOMAINS = {
    "crescendo-consulting.net",  # Their company domain
    "horizontech.com",  # HR Policy Bot demo client
}

# Domains that are NEVER real leads — always system/noise
NOISE_DOMAINS = {
    "hubspot.com",  # HubSpot sample/system contacts
    "delivery-status.com",
    "microsoft.com",  # DMARC reports, security alerts
    "airbnb.com",  # Receipts, notifications
    "ibm.com",  # Recruiting
    "cal.com",  # Calendly signup
    "aitinkerers.org",  # AI tinkerers group
    "google.com",  # Google notifications
    "googlemail.com",
    "stripe.com",  # Stripe receipts
    "intercom.io", "intercom.com",
    "docusign.net", "docusign.com",
    "slack.com",
    "notion.so",
    "github.com",
    "linkedin.com",
    "amazon.com", "amazonses.com",
    "anthropic.com",  # Anthropic notifications
    "openai.com",
    "zoom.us",
    "calendly.com",
    "atlassian.com",
}

# Local-parts (before @) that are ALWAYS system/no-reply
NOISE_LOCAL_PARTS = {
    "noreply", "no-reply", "donotreply", "do-not-reply",
    "support", "help", "info", "hello", "team", "admin",
    "receipts", "billing", "invoices", "notifications",
    "alerts", "security", "abuse",
    "moo", "dmarcreport", "googlecloud", "workspace",
    "talent", "recruiting", "careers",
}


def is_noise(email: str) -> bool:
    """Return True only if the contact is OBVIOUSLY system/noise."""
    e = email.lower().strip()
    local, _, domain = e.partition("@")
    if not local or not domain:
        return False
    # Match by exact domain
    if domain in NOISE_DOMAINS:
        return True
    # Match by exact local part (and 'receipts+something@stripe.com' patterns)
    base_local = local.split("+")[0]
    if base_local in NOISE_LOCAL_PARTS:
        return True
    return False


def neon_emails() -> set[str]:
    """All primary_emails from Neon — these are real scraped leads."""
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT LOWER(primary_email)
        FROM businesses
        WHERE primary_email IS NOT NULL AND primary_email != ''
    """)
    emails = {row[0] for row in cur.fetchall()}
    conn.close()
    return emails


def hubspot_contacts() -> list[dict]:
    """All HubSpot contacts with id + email."""
    all_contacts = []
    after = None
    while True:
        body = {"limit": 100, "properties": ["email"]}
        if after: body["after"] = after
        r = requests.post("https://api.hubapi.com/crm/v3/objects/contacts/search",
                          headers=HEADERS, json=body, timeout=20)
        data = r.json()
        all_contacts.extend(data.get("results", []))
        if not data.get("paging", {}).get("next", {}).get("after"):
            break
        after = data["paging"]["next"]["after"]
    return all_contacts


def is_whitelisted(email: str) -> bool:
    e = email.lower()
    if e in WHITELIST_EMAILS:
        return True
    domain = e.split("@")[-1]
    return domain in WHITELIST_DOMAINS


def main() -> int:
    apply_changes = os.environ.get("CLEANUP_APPLY") == "1"

    print("Loading data...")
    neon = neon_emails()
    print(f"  Neon has {len(neon)} unique scraped emails")

    contacts = hubspot_contacts()
    print(f"  HubSpot has {len(contacts)} contacts")

    # Categorize. NEW STRATEGY: only delete OBVIOUS noise (system/no-reply
    # domains + role addresses). Anything with a person-name @ business-domain
    # gets KEPT even if not in Neon — it might be a real lead Neon doesn't
    # currently track.
    keep_neon = []
    keep_whitelist = []
    keep_business_looking = []
    delete = []
    no_email = []

    for c in contacts:
        email = (c["properties"].get("email") or "").lower().strip()
        if not email:
            no_email.append(c)
            continue
        if is_whitelisted(email):
            keep_whitelist.append(c)
        elif email in neon:
            keep_neon.append(c)
        elif is_noise(email):
            delete.append(c)
        else:
            keep_business_looking.append(c)

    print(f"\nBreakdown:")
    print(f"  Keep (in Neon cohort):           {len(keep_neon)}")
    print(f"  Keep (whitelisted):              {len(keep_whitelist)}")
    print(f"  Keep (business-looking, no match in Neon): {len(keep_business_looking)}")
    print(f"  Skip (no email):                 {len(no_email)}")
    print(f"  ⚠️  DELETE (clear system/noise): {len(delete)}")
    print(f"  Total after cleanup:             {len(contacts) - len(delete)}")

    # Show 20 example deletions
    print(f"\nSample 20 deletion candidates:")
    for c in delete[:20]:
        print(f"  - {c['properties'].get('email')}")

    if not apply_changes:
        print(f"\n  DRY RUN. Set CLEANUP_APPLY=1 to actually delete {len(delete)} contacts.")
        return 0

    if not delete:
        print("\nNothing to delete.")
        return 0

    # Batch-delete in chunks of 100
    print(f"\nDeleting {len(delete)} contacts in batches...")
    deleted = 0
    errored = 0
    for i in range(0, len(delete), 100):
        chunk = delete[i:i+100]
        body = {"inputs": [{"id": c["id"]} for c in chunk]}
        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/contacts/batch/archive",
            headers=HEADERS, json=body, timeout=30,
        )
        if r.status_code in (200, 204):
            deleted += len(chunk)
            print(f"  Batch {i//100 + 1}: deleted {len(chunk)}")
        else:
            errored += len(chunk)
            print(f"  Batch {i//100 + 1} FAILED: {r.status_code} {r.text[:200]}")

    print(f"\nDone. Deleted {deleted}, errored {errored}.")
    print(f"HubSpot contact count should now be ~{len(contacts) - deleted}")
    print(f"(All deleted contacts are recoverable from HubSpot's recycle bin for 90 days.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
