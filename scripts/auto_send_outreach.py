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
import json
import logging
import os
import smtplib
import sys
import time
from datetime import date, datetime
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("autosend")

TOKEN = os.environ['HUBSPOT_ACCESS_TOKEN']
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

LIST_ID = os.environ.get("HUBSPOT_LIST_ID", "17")
LIMIT = int(os.environ.get("LIMIT", "25"))
DELAY_SEC = int(os.environ.get("DELAY_SEC", "60"))
DRY_RUN = os.environ.get("DRY_RUN", "1") == "1"
BCC_HUBSPOT = os.environ.get("BCC_HUBSPOT", "246276084@bcc.na2.hubspot.com")
HARD_STOP_ON_ERROR = os.environ.get("HARD_STOP_ON_ERROR", "1") == "1"

# SMTP credentials (App Password — no OAuth needed)
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "").replace(" ", "")

# Sender identity — gavin (default), joanna, or custom via env
SENDER = os.environ.get("SENDER", "gavin").lower()
SENDERS = {
    "gavin": {
        "name": "Gavin Coleman",
        "signature": "Gavin Coleman\nCrescendo Consulting",
        "reply_to": os.environ.get("REPLY_TO", ""),  # default: use FROM (Gmail account)
    },
    "joanna": {
        "name": "Joanna Webber",
        "signature": "Joanna Webber\nCrescendo Consulting",
        "reply_to": os.environ.get("REPLY_TO", "jo.webber@crescendo-consulting.net"),
    },
}
SENDER_CONFIG = SENDERS.get(SENDER, SENDERS["gavin"])

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


# Map Business Vertical dropdown values to natural-language phrases that
# slot cleanly into "For ___ like yours, ..."
VERTICAL_PHRASE = {
    "Restaurant": "restaurants",
    "Cafe / Bar / Brewery": "hospitality businesses",
    "Hospitality (hotel, B&B)": "hospitality businesses",
    "Retail / E-commerce": "retail businesses",
    "Health & Wellness (salon, spa, gym)": "wellness businesses",
    "Professional Services": "professional services firms",
    "Home Services": "home-services businesses",
    "Healthcare / Dental": "healthcare practices",
    "Real Estate": "real estate businesses",
    "Other": "businesses",
}


def compose_email(contact: dict) -> dict:
    p = contact["properties"]
    co = contact["_company"]
    first = (p.get("firstname") or "there").strip()
    company_name = (co.get("name") or "your business").strip()
    vertical = (co.get("business_vertical") or "Other").strip()
    phrase = VERTICAL_PHRASE.get(vertical, "businesses")

    subject = f"A diagnostic on {company_name}"
    body = f"""Hi {first},

We're ex-Big 4 consultants and we built a platform that does the diagnostic work we used to charge a fortune for. For {phrase} like yours, we routinely find north of $100K in combined risk and opportunity.

Our platform evaluates the four pillars most important to your top and bottom line:
• Security gaps in your tech stack and site
• Key complaint themes driving customers away
• Your visibility on Google and AI search
• How you stack up against competitors — where you're winning, where you're losing

Run it on a real business in your industry and see for yourself:
{PLATFORM_URL}

Want one for {company_name}? Just reply.

{SENDER_CONFIG['signature']}"""
    return {"subject": subject, "body": body, "to": p.get("email") or ""}


def send_via_smtp(smtp, to: str, subject: str, body: str) -> str:
    """Send an email through Gmail SMTP using an App Password.
    Returns a pseudo message-id for logging.
    """
    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = formataddr((SENDER_CONFIG["name"], GMAIL_ADDRESS))
    msg["To"] = to
    msg["Subject"] = subject
    if SENDER_CONFIG.get("reply_to"):
        msg["Reply-To"] = SENDER_CONFIG["reply_to"]

    # BCC is handled at the envelope level (recipients list), not a header,
    # so the recipient doesn't see the HubSpot logging address.
    recipients = [to, BCC_HUBSPOT]
    smtp.sendmail(GMAIL_ADDRESS, recipients, msg.as_string())
    return f"smtp-{datetime.utcnow().strftime('%H%M%S')}-{to.split('@')[0]}"


def log_send(record: dict) -> None:
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def main() -> int:
    mode = "🟢 LIVE SEND" if not DRY_RUN else "🟡 DRY RUN (no emails sent)"
    log.info("=" * 60)
    log.info(f"Mode: {mode}")
    log.info(f"Sender: {SENDER_CONFIG['name']} ({SENDER})")
    log.info(f"List: {LIST_ID}  Limit: {LIMIT}  Delay: {DELAY_SEC}s")
    if SENDER_CONFIG.get("reply_to"):
        log.info(f"Reply-To: {SENDER_CONFIG['reply_to']}")
    else:
        log.info(f"Reply-To: (none — replies go to your Gmail)")
    log.info(f"BCC: {BCC_HUBSPOT} (HubSpot auto-log)")
    log.info("=" * 60)

    # SMTP connection (App Password — no OAuth)
    smtp = None
    sender_email = GMAIL_ADDRESS
    if not DRY_RUN:
        if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
            log.error("GMAIL_ADDRESS and GMAIL_APP_PASSWORD must be set in .env")
            log.error("Generate an app password at myaccount.google.com/apppasswords")
            return 1
        log.info(f"Connecting to Gmail SMTP as {GMAIL_ADDRESS}...")
        try:
            smtp = smtplib.SMTP("smtp.gmail.com", 587, timeout=30)
            smtp.starttls()
            smtp.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            log.info("✅ SMTP authenticated")
        except Exception as e:
            log.error(f"SMTP login failed: {e}")
            log.error("Check GMAIL_ADDRESS + GMAIL_APP_PASSWORD. App passwords need 2FA enabled.")
            return 1

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
        print(f"   - Sender (FROM): {SENDER_CONFIG['name']} <{sender_email}>")
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
            msg_id = send_via_smtp(smtp, to, email_data["subject"], email_data["body"])
            sent += 1
            log.info(f"[{i}/{len(contacts)}] ✅ Sent to {to}  (msg={msg_id})")
            log_send({
                "ts": datetime.utcnow().isoformat(),
                "to": to,
                "subject": email_data["subject"],
                "msg_id": msg_id,
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

    if smtp:
        try:
            smtp.quit()
        except Exception:
            pass

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
