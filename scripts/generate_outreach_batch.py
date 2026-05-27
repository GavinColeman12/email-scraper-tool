#!/usr/bin/env python3
"""Generate a batch of personalized cold-outreach emails + LinkedIn connection
notes for a HubSpot list. Outputs a single HTML file Joanna opens in her
browser; each lead has:

  - 📧 "Open in Gmail" button — clicks open a pre-filled Gmail compose with
    subject + personalized body + recipient. She glances, hits Send.
  - 💼 LinkedIn note (300-char max) with a copy button + clickable LinkedIn URL.
  - Quick context: vertical, lead score, key data points.

Usage:
    cd /Users/gavincoleman/Downloads/email-scraper

    # Default — UK Priority list (id=17)
    python3 scripts/generate_outreach_batch.py

    # Specific HubSpot list
    HUBSPOT_LIST_ID=18 python3 scripts/generate_outreach_batch.py

    # Filter to top N by lead score
    HUBSPOT_LIST_ID=17 LIMIT=10 python3 scripts/generate_outreach_batch.py
"""

from __future__ import annotations
import html
import logging
import os
import sys
from datetime import date
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("outreach")

TOKEN = os.environ['HUBSPOT_ACCESS_TOKEN']
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

LIST_ID = os.environ.get("HUBSPOT_LIST_ID", "17")  # default: UK Priority
LIMIT = int(os.environ.get("LIMIT", "100"))
PLATFORM_URL = "https://crescendo-platform.crescendo-consulting.net/explore"


def fetch_list_members(list_id: str) -> list[dict]:
    """Pull contact records for the given HubSpot list."""
    members = []
    after = None
    while True:
        url = f"https://api.hubapi.com/crm/v3/lists/{list_id}/memberships"
        params = {"limit": 100}
        if after:
            params["after"] = after
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        if r.status_code != 200:
            log.error("List fetch failed: %s %s", r.status_code, r.text[:200])
            return []
        data = r.json()
        members.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    if not members:
        return []

    # Hydrate with contact + company properties
    contact_ids = [m.get("recordId") for m in members][:LIMIT]
    body = {
        "inputs": [{"id": cid} for cid in contact_ids],
        "properties": [
            "email", "firstname", "lastname", "jobtitle", "phone",
            "hs_linkedin_url", "city", "state",
            "email_source_category", "lead_quality_score_raw",
            "associatedcompanyid",
        ],
        "associations": ["companies"],
    }
    r = requests.post("https://api.hubapi.com/crm/v3/objects/contacts/batch/read",
                      headers=HEADERS, json=body, timeout=30)
    contacts = r.json().get("results", [])

    # Pull company info for each contact's associated company
    company_ids = set()
    for c in contacts:
        for assoc in (c.get("associations", {}).get("companies", {}).get("results") or []):
            company_ids.add(assoc["id"])

    companies = {}
    if company_ids:
        body = {
            "inputs": [{"id": cid} for cid in company_ids],
            "properties": ["name", "domain", "business_vertical", "city", "state", "description"],
        }
        r = requests.post("https://api.hubapi.com/crm/v3/objects/companies/batch/read",
                          headers=HEADERS, json=body, timeout=30)
        for co in r.json().get("results", []):
            companies[co["id"]] = co["properties"]

    # Attach company to each contact
    for c in contacts:
        assoc = c.get("associations", {}).get("companies", {}).get("results") or []
        if assoc:
            c["_company"] = companies.get(assoc[0]["id"], {})
        else:
            c["_company"] = {}

    return contacts


def compose_email(contact: dict) -> dict:
    """Generate subject + body for a single contact."""
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


def compose_linkedin_note(contact: dict) -> str:
    """Generate a personalized LinkedIn connection note (300 char max).

    LinkedIn imposes a 300-character limit on connection-request notes.
    """
    p = contact["properties"]
    co = contact["_company"]
    first = (p.get("firstname") or "").strip()
    company = (co.get("name") or "").strip()
    title = (p.get("jobtitle") or "").strip()

    note = (f"Hi {first} — saw your work at {company}. We're ex-Big 4 consultants "
            f"who built a platform that surfaces $100K+ in risk/opportunity for "
            f"businesses like yours. Would love to connect and share a quick demo "
            f"if relevant. — Joanna")
    # Trim to 300 chars max
    if len(note) > 295:
        note = note[:292] + "..."
    return note


def render_html(contacts: list[dict], list_name: str) -> str:
    """Build the HTML page Joanna opens in her browser."""
    rows = []
    for i, c in enumerate(contacts, 1):
        p = c["properties"]
        co = c["_company"]
        email_data = compose_email(c)
        li_note = compose_linkedin_note(c)
        li_url = (p.get("hs_linkedin_url") or "").strip()
        contact_email = (p.get("email") or "").strip()
        gmail_url = (
            f"https://mail.google.com/mail/?view=cm&fs=1"
            f"&to={quote(contact_email)}"
            f"&su={quote(email_data['subject'])}"
            f"&body={quote(email_data['body'])}"
        )
        score = p.get("lead_quality_score_raw") or "—"
        first = p.get("firstname") or ""
        last = p.get("lastname") or ""
        rows.append(f"""
<div class="lead">
  <div class="head">
    <div>
      <h3>{html.escape(first)} {html.escape(last)}</h3>
      <div class="meta">{html.escape(p.get('jobtitle') or '?')} · {html.escape(co.get('name') or '?')} · {html.escape(co.get('business_vertical') or '?')}</div>
      <div class="meta-small">{html.escape(contact_email)} · Score: {score}</div>
    </div>
    <div class="num">#{i}/{len(contacts)}</div>
  </div>

  <div class="actions">
    <a href="{gmail_url}" target="_blank" class="btn-primary">📧 Open in Gmail</a>
    {f'<a href="{html.escape(li_url)}" target="_blank" class="btn-secondary">💼 Open LinkedIn</a>' if li_url else '<span class="btn-disabled">No LinkedIn URL</span>'}
    <button onclick="copyText(this, `{html.escape(li_note).replace('`', '')}`)" class="btn-secondary">📋 Copy LinkedIn note</button>
  </div>

  <details>
    <summary>Email preview</summary>
    <div class="preview"><b>Subject:</b> {html.escape(email_data['subject'])}<br><br>{html.escape(email_data['body']).replace(chr(10), '<br>')}</div>
  </details>

  <details>
    <summary>LinkedIn note (300 char max)</summary>
    <div class="preview">{html.escape(li_note)} <span class="char-count">({len(li_note)} chars)</span></div>
  </details>
</div>
""")

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Outreach Batch — {date.today()}</title>
<style>
  body {{ font-family: -apple-system, sans-serif; max-width: 900px; margin: 2rem auto; padding: 1rem; background: #f5f5f5; }}
  h1 {{ font-size: 1.5rem; color: #1B6B4A; }}
  .summary {{ background: white; padding: 1rem; border-radius: 8px; margin-bottom: 1rem; }}
  .lead {{ background: white; padding: 1rem; margin-bottom: 1rem; border-radius: 8px; border-left: 4px solid #1B6B4A; }}
  .head {{ display: flex; justify-content: space-between; align-items: start; }}
  .head h3 {{ margin: 0; font-size: 1.1rem; }}
  .meta {{ color: #555; font-size: 0.9rem; }}
  .meta-small {{ color: #888; font-size: 0.8rem; margin-top: 0.25rem; }}
  .num {{ color: #aaa; font-size: 0.9rem; }}
  .actions {{ display: flex; gap: 0.5rem; margin: 0.75rem 0; flex-wrap: wrap; }}
  .btn-primary {{ background: #1B6B4A; color: white; padding: 0.5rem 1rem; border-radius: 6px; text-decoration: none; font-size: 0.9rem; border: none; cursor: pointer; }}
  .btn-secondary {{ background: #e5e7eb; color: #1f2937; padding: 0.5rem 1rem; border-radius: 6px; text-decoration: none; font-size: 0.9rem; border: none; cursor: pointer; }}
  .btn-disabled {{ background: #f3f4f6; color: #9ca3af; padding: 0.5rem 1rem; border-radius: 6px; font-size: 0.9rem; }}
  details {{ margin-top: 0.5rem; }}
  summary {{ cursor: pointer; color: #1B6B4A; font-size: 0.9rem; }}
  .preview {{ background: #f9fafb; padding: 0.75rem; border-radius: 4px; margin-top: 0.5rem; font-size: 0.85rem; line-height: 1.5; white-space: pre-wrap; }}
  .char-count {{ color: #888; }}
</style>
</head><body>
<h1>📨 Outreach Batch — {date.today()}</h1>
<div class="summary">
  <b>List:</b> {html.escape(list_name)} (id={LIST_ID}) — {len(contacts)} contacts<br>
  <b>Platform demo URL:</b> {PLATFORM_URL}<br>
  <b>How to use:</b> For each lead, click 📧 to open Gmail with the email pre-filled. Glance, click Send. Optionally click 💼 to open LinkedIn and paste the connection note.
</div>
{''.join(rows)}
<script>
function copyText(btn, text) {{
  navigator.clipboard.writeText(text).then(() => {{
    const old = btn.textContent;
    btn.textContent = '✓ Copied!';
    setTimeout(() => btn.textContent = old, 1500);
  }});
}}
</script>
</body></html>"""


def main() -> int:
    log.info(f"Pulling HubSpot list {LIST_ID} (limit {LIMIT})...")
    contacts = fetch_list_members(LIST_ID)
    if not contacts:
        log.error("No contacts found.")
        return 1

    contacts = contacts[:LIMIT]
    log.info(f"Got {len(contacts)} contacts. Generating outreach batch...")

    list_name = f"List {LIST_ID}"  # We don't query the list's name for simplicity

    html_content = render_html(contacts, list_name)
    out_dir = Path(__file__).resolve().parent.parent / "exports"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"outreach_batch_{date.today().isoformat()}_list{LIST_ID}.html"
    out_path.write_text(html_content, encoding="utf-8")

    log.info(f"✅ Generated {out_path}")
    log.info(f"Open it in your browser:")
    log.info(f"  open {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
