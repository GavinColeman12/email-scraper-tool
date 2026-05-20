"""High-level orchestrator: turn a scraper business dict into Company +
Contact + Deal in HubSpot. Single public entry point: sync_lead_to_hubspot.

Knows business dict shape and HubSpot property internal names — both
documented in `notes/hubspot_property_names.md`.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

from src.hubspot_client import HubSpotClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SyncResult:
    company_id: Optional[str]
    contact_id: Optional[str]
    deal_id: Optional[str]
    error: Optional[str]


def normalize_score_to_1_10(score_0_100: int) -> int:
    """Map an existing 0-100 lead quality score to HubSpot's 1-10 property.
    Clamps out-of-range inputs."""
    normalized = round(score_0_100 / 10)
    return max(1, min(10, normalized))


def _classify_role(title: Optional[str]) -> str:
    """Map a free-text title to our Role dropdown values."""
    if not title:
        return "Other"
    t = title.lower()
    if "owner" in t or "founder" in t or "ceo" in t or "president" in t:
        return "Owner"
    if "gm" in t or "general manager" in t:
        return "GM"
    if "operations" in t or "ops" in t:
        return "Operations Manager"
    if "marketing" in t:
        return "Marketing Director"
    return "Other"


def _extract_domain(website: Optional[str]) -> Optional[str]:
    if not website:
        return None
    parsed = urlparse(website if "://" in website else f"https://{website}")
    host = (parsed.netloc or parsed.path).lower()
    return re.sub(r"^www\.", "", host) or None


def sync_lead_to_hubspot(
    business: dict,
    lead_score_0_100: int,
    client: Optional[HubSpotClient] = None,
) -> SyncResult:
    """Sync one fully-processed business to HubSpot as Company + Contact + Deal.

    Idempotent on Company (by domain) and Contact (by email). Creates a new
    Deal at "Cold Outreach" stage every time — dedup of deals not attempted.
    """
    if client is None:
        token = os.environ.get("HUBSPOT_ACCESS_TOKEN")
        if not token:
            return SyncResult(None, None, None, "HUBSPOT_ACCESS_TOKEN not set")
        client = HubSpotClient(access_token=token)

    domain = _extract_domain(business.get("website"))
    if not domain:
        return SyncResult(None, None, None, "Missing or unparseable website")

    email = business.get("picked_email")
    if not email:
        return SyncResult(None, None, None, "Missing picked_email")

    try:
        company_id = client.upsert_company(
            domain=domain,
            properties={
                "name": business.get("business_name") or domain,
                "domain": domain,
                "city": business.get("city") or "",
                "state": business.get("state") or "",
                "cuisine_type": business.get("cuisine_type") or "Other",
                "num_locations": business.get("num_locations") or 1,
                "source_list_batch": business.get("source_batch") or "manual",
                "audit_url": business.get("audit_url") or "",
            },
        )

        contact_id = client.upsert_contact(
            email=email,
            properties={
                "email": email,
                "firstname": business.get("picked_first_name") or "",
                "lastname": business.get("picked_last_name") or "",
                "jobtitle": business.get("picked_title") or "",
                "phone": business.get("phone") or "",
                "role": _classify_role(business.get("picked_title")),
                "lead_score": normalize_score_to_1_10(lead_score_0_100),
            },
        )

        client.associate_contact_to_company(contact_id=contact_id, company_id=company_id)

        pipeline_id = os.environ.get("HUBSPOT_PIPELINE_ID")
        cold_stage_id = os.environ.get("HUBSPOT_STAGE_COLD_OUTREACH")
        if not pipeline_id or not cold_stage_id:
            return SyncResult(
                company_id,
                contact_id,
                None,
                "Missing HUBSPOT_PIPELINE_ID or HUBSPOT_STAGE_COLD_OUTREACH",
            )

        deal_id = client.create_deal(
            properties={
                "dealname": f"{business.get('business_name') or domain} — Audit Outreach",
                "dealstage": cold_stage_id,
                "pipeline": pipeline_id,
                "deal_source": "Cold email",
            },
            company_id=company_id,
            contact_id=contact_id,
        )

        return SyncResult(
            company_id=company_id, contact_id=contact_id, deal_id=deal_id, error=None
        )

    except Exception as exc:
        logger.exception("HubSpot sync failed for %s", email)
        return SyncResult(None, None, None, str(exc))
