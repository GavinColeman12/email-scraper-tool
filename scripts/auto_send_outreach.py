#!/usr/bin/env python3
"""Auto-send personalized cold outreach to a HubSpot list via Gmail API.

For each contact in the target HubSpot list:
  1. Generate personalized email (uses business_vertical, name, company)
  2. Send via Gmail API as the authenticated user
  3. BCC HubSpot's logging address — auto-logs to contact's CRM timeline
  4. Set Reply-To to JOANNA so replies route to her
  5. Wait DELAY_SEC between sends (default 60s — looks human)
  6. Stop on first error to prevent runaway sends

Safety:
  - DRY_RUN=1 by default (prints what would be sent, doesn't send)
  - LIMIT caps the batch (default 25 — be conservative)
  - HARD_STOP_ON_ERROR=1 by default — stops if any send fails
  - Logs every send action to exports/send_log_<date>.jsonl

Usage:
    cd /Users/gavincoleman/Downloads/email-scraper

    # Dry-run (default — safe to run anytime)
    python3 scripts/auto_send_outreach.py

    # Actually send (only after dry-run looks good)
    DRY_RUN=0 python3 scripts/auto_send_outreach.py

    # Different list, lower limit, faster pace
    HUBSPOT_LIST_ID=17 LIMIT=10 DELAY_SEC=30 DRY_RUN=0 python3 scripts/auto_send_outreach.py

Env vars:
    HUBSPOT_LIST_ID     default 17 (UK Priority)
    LIMIT               default 25 — max emails per run
    DELAY_SEC           default 60 — pause between sends
    DRY_RUN             default 1 — set 0 to actually send
    REPLY_TO            default jo.webber@crescendo-consulting.net
    BCC_HUBSPOT         default 246276084@bcc.na2.hubspot.com
"""

from __future__ import annotations
import base64
import json
import logging
import os
import sys
import time
from datetime import date, datetime
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests
from src.gmail_client import get_gmail_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("autosend")

TOKEN = os.environ['HUBSPOT_ACCESS_TOKEN']
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

LIST_ID = os.environ.get("HUBSPOT_LIST_ID", "17")
LIMIT = int(os.environ.get("LIMIT", "25"))
DELAY_SEC = int(os.environ.get("DELAY_SEC", "60"))
DRY_RUN = os.environ.get("DRY_RUN", "1") == "1"
REPLY_TO = os.environ.get("REPLY_TO", "jo.webber@crescendo-consulting.net")
BCC_HUBSPOT = os.environ.get("BCC_HUBSPOT", "246276084@bcc.na2.hubspot.com")
HARD_STOP_ON_ERROR = os.environ.get("HARD_STOP_ON_ERROR", "1") == "1"

PLATFORM_URL = "https://crescendo-platform.crescendo-consulting.net/explore"

LOG_DIR = Path(__file__).resolve().parent.parent / "exports"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"send_log_{date.today().isoformat()}.jsonl"


def fetch_list_members(list_id: str) -> list[dict]:
    """Pull contacts from a HubSpot list with full personalization data."""
    members = []
    after = None
    while True:
        url = f"https://api.hubapi.com/crm/v3/lists/{list_id}/memberships"
        params = {"limit": 100}
        if after: params["after"] = after
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        data = r.json()
        members.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after: break

    if not members:
        return []

    contact_ids = [m.get("recordId") for m in members][:LIMIT]

    # Step 1: contact properties (batch_read doesn't return associations reliably)
    body = {
        "inputs": [{"id": cid} for cid in contact_ids],
        "properties": ["email", "firstname", "lastname", "jobtitle"],
    }
    r = requests.post("https://api.hubapi.com/crm/v3/objects/contacts/batch/read",
                      headers=HEADERS, json=body, timeout=30)
    contacts = r.json().get("results", [])

    # Step 2: associations via v4 batch endpoint
    body = {"inputs": [{"id": cid} for cid in contact_ids]}
    r = requests.post("https://api.hubapi.com/crm/v4/associations/contacts/companies/batch/read",
                      headers=HEADERS, json=body, timeout=30)
    assoc_results = r.json().get("results", [])
    # Map contact_id → first associated company_id
    contact_to_company = {}
    for ar in assoc_results:
        from_id = ar.get("from", {}).get("id")
        to_list = ar.get("to") or []
        if from_id and to_list:
            contact_to_company[from_id] = to_list[0].get("toObjectId")

    # Step 3: fetch company properties for all referenced companies
    company_ids = set(contact_to_company.values())
    companies = {}
    if company_ids:
        body = {"inputs": [{"id": cid} for cid in company_ids],
                "properties": ["name", "business_vertical"]}
        r = requests.post("https://api.hubapi.com/crm/v3/objects/companies/batch/read",
                          headers=HEADERS, json=body, timeout=30)
        for co in r.json().get("results", []):
            companies[co["id"]] = co["properties"]

    # Step 4: attach company data to each contact
    for c in contacts:
        cid = c["id"]
        company_id = contact_to_company.get(cid)
        c["_company"] = companies.get(str(company_id), {}) if company_id else {}

    return contacts


def compose_email(contact: dict) -> dict:
    p = contact["properties"]
    co = contact["_company"]
    first = (p.get("firstname") or "there").strip()
    company_name = (co.get("name") or "your business").strip()
    vertical = (co.get("business_vertical") or "service").strip()

    subject = f"A diagnostic on {company_name}"
    body = f"""Hi {first},

We're ex-Big 4 consultants and we built a platform that does the diagnostic work we used to charge a fortune for. For {vertical} businesses like yours, we routinely find north of $100K in combined risk and opportunity.

Our platform evaluates the four pillars most important to your top and bottom line:
• Security gaps in your tech stack and site
• Key complaint themes driving customers away
• Your visibility on Google and AI search
• How you stack up against competitors — where you're winning, where you're losing

Run it on a real business in your industry and see for yourself:
{PLATFORM_URL}

Want one for {company_name}? Just reply.

Joanna Webber
Crescendo Consulting"""
    return {"subject": subject, "body": body, "to": p.get("email") or ""}


def send_via_gmail(service, to: str, subject: str, body: str) -> str:
    """Send an email through the authenticated Gmail account.
    Returns the Gmail message ID.
    """
    msg = MIMEText(body, "plain", "utf-8")
    msg["To"] = to
    msg["Subject"] = subject
    msg["Reply-To"] = REPLY_TO
    msg["Bcc"] = BCC_HUBSPOT

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("ascii")
    result = service.users().messages().send(
        userId="me", body={"raw": raw}
    ).execute()
    return result.get("id", "?")


def log_send(record: dict) -> None:
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def main() -> int:
    mode = "🟢 LIVE SEND" if not DRY_RUN else "🟡 DRY RUN (no emails sent)"
    log.info("=" * 60)
    log.info(f"Mode: {mode}")
    log.info(f"List: {LIST_ID}  Limit: {LIMIT}  Delay between sends: {DELAY_SEC}s")
    log.info(f"Reply-To: {REPLY_TO}")
    log.info(f"BCC: {BCC_HUBSPOT} (HubSpot auto-log)")
    log.info("=" * 60)

    # Gmail service
    service = None
    if not DRY_RUN:
        log.info("Authenticating Gmail...")
        service = get_gmail_service()
        if not service:
            log.error("Failed to authenticate Gmail. Check credentials.")
            return 1
        # Get sending account email for the log
        profile = service.users().getProfile(userId="me").execute()
        sender_email = profile.get("emailAddress", "?")
        log.info(f"Authenticated as: {sender_email}")

    # Pull contacts
    log.info(f"Fetching HubSpot list {LIST_ID}...")
    contacts = fetch_list_members(LIST_ID)
    if not contacts:
        log.error("No contacts found in list.")
        return 1
    contacts = contacts[:LIMIT]
    log.info(f"Got {len(contacts)} contacts to process.")

    if not DRY_RUN:
        # Final confirmation prompt
        print()
        print(f"⚠️  About to SEND {len(contacts)} emails to real people.")
        print(f"   - Sender (FROM): authenticated Gmail account ({sender_email})")
        print(f"   - Reply-To: {REPLY_TO}")
        print(f"   - BCC: {BCC_HUBSPOT}")
        print(f"   - Pace: 1 email every {DELAY_SEC} seconds = ~{(len(contacts) * DELAY_SEC) // 60} min total")
        print(f"   - All sends logged to: {LOG_FILE}")
        print()
        confirm = input("Type 'YES SEND' to proceed: ")
        if confirm.strip() != "YES SEND":
            log.info("Aborted.")
            return 0

    sent = 0
    errored = 0
    for i, c in enumerate(contacts, 1):
        email_data = compose_email(c)
        to = email_data["to"]
        if not to:
            log.warning(f"[{i}/{len(contacts)}] Contact missing email — skipping")
            continue

        if DRY_RUN:
            log.info(f"[{i}/{len(contacts)}] WOULD SEND to {to}")
            log.info(f"    Subject: {email_data['subject']}")
            log.info(f"    Body preview: {email_data['body'][:100]}...")
            log_send({
                "ts": datetime.utcnow().isoformat(),
                "dry_run": True,
                "to": to,
                "subject": email_data["subject"],
            })
            continue

        try:
            msg_id = send_via_gmail(service, to, email_data["subject"], email_data["body"])
            sent += 1
            log.info(f"[{i}/{len(contacts)}] ✅ Sent to {to}  (Gmail msg_id={msg_id})")
            log_send({
                "ts": datetime.utcnow().isoformat(),
                "to": to,
                "subject": email_data["subject"],
                "gmail_msg_id": msg_id,
                "status": "sent",
            })
        except Exception as e:
            errored += 1
            log.error(f"[{i}/{len(contacts)}] ❌ Failed to send to {to}: {e}")
            log_send({
                "ts": datetime.utcnow().isoformat(),
                "to": to,
                "subject": email_data["subject"],
                "status": "error",
                "error": str(e),
            })
            if HARD_STOP_ON_ERROR:
                log.error("HARD_STOP_ON_ERROR=1 — stopping batch.")
                break

        # Pace
        if i < len(contacts):
            log.info(f"    Waiting {DELAY_SEC}s before next send...")
            time.sleep(DELAY_SEC)

    log.info("=" * 60)
    if DRY_RUN:
        log.info(f"DRY RUN complete. {len(contacts)} emails WOULD be sent.")
        log.info(f"Re-run with DRY_RUN=0 to actually send.")
    else:
        log.info(f"Done. Sent: {sent}, Errored: {errored}")
    log.info(f"Log: {LOG_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
