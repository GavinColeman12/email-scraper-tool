"""Tests for sync_lead_to_hubspot orchestrator. Mocks HubSpotClient
to verify the right calls are made with the right arguments."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.hubspot_sync import SyncResult, sync_lead_to_hubspot


@pytest.fixture(autouse=True)
def _hubspot_env(monkeypatch):
    """Default env vars for HubSpot sync. Individual tests can override or delete."""
    monkeypatch.setenv("HUBSPOT_PIPELINE_ID", "pipe-test")
    monkeypatch.setenv("HUBSPOT_STAGE_COLD_OUTREACH", "stage-cold-test")


@pytest.fixture
def sample_business() -> dict:
    return {
        "business_name": "Joe's Pizza",
        "website": "https://joespizza.com",
        "picked_email": "joe@joespizza.com",
        "picked_first_name": "Joe",
        "picked_last_name": "Pizzaro",
        "picked_title": "Owner",
        "phone": "+15551234",
        "city": "Brooklyn",
        "state": "NY",
        "industry": "Restaurant",
        "num_locations": 3,
        "source_batch": "brooklyn-pizza-2026-05",
        "audit_url": "https://audits.example.com/joes-pizza",
    }


def test_sync_lead_creates_company_contact_and_deal(sample_business):
    client = MagicMock()
    client.upsert_company.return_value = "co-100"
    client.upsert_contact.return_value = "c-200"
    client.create_deal.return_value = "d-300"

    result = sync_lead_to_hubspot(
        business=sample_business, lead_score_0_100=72, client=client
    )

    assert result == SyncResult(
        company_id="co-100", contact_id="c-200", deal_id="d-300", error=None
    )
    client.upsert_company.assert_called_once()
    client.upsert_contact.assert_called_once()
    client.associate_contact_to_company.assert_called_once_with(
        contact_id="c-200", company_id="co-100"
    )
    client.create_deal.assert_called_once()


def test_sync_lead_maps_business_to_company_properties(sample_business):
    client = MagicMock()
    client.upsert_company.return_value = "co-100"
    client.upsert_contact.return_value = "c-200"
    client.create_deal.return_value = "d-300"

    sync_lead_to_hubspot(business=sample_business, lead_score_0_100=72, client=client)

    company_call = client.upsert_company.call_args
    assert company_call.kwargs["domain"] == "joespizza.com"
    props = company_call.kwargs["properties"]
    assert props["name"] == "Joe's Pizza"
    assert props["domain"] == "joespizza.com"
    assert props["business_vertical"] == "Restaurant"
    assert props["of_locations"] == 3
    assert props["source__list_batch"] == "brooklyn-pizza-2026-05"
    assert props["audit_url"] == "https://audits.example.com/joes-pizza"
    assert props["city"] == "Brooklyn"
    assert props["state"] == "NY"


def test_sync_lead_maps_business_to_contact_properties(sample_business):
    client = MagicMock()
    client.upsert_company.return_value = "co-100"
    client.upsert_contact.return_value = "c-200"
    client.create_deal.return_value = "d-300"

    sync_lead_to_hubspot(business=sample_business, lead_score_0_100=72, client=client)

    contact_call = client.upsert_contact.call_args
    props = contact_call.kwargs["properties"]
    assert props["email"] == "joe@joespizza.com"
    assert props["firstname"] == "Joe"
    assert props["lastname"] == "Pizzaro"
    assert props["jobtitle"] == "Owner"
    assert props["phone"] == "+15551234"
    assert props["role"] == "Owner"
    # 72/100 normalized to 1-10 = round(7.2) = 7
    assert props["lead_score"] == 7


def test_sync_lead_creates_deal_at_cold_outreach_stage(sample_business):
    client = MagicMock()
    client.upsert_company.return_value = "co-100"
    client.upsert_contact.return_value = "c-200"
    client.create_deal.return_value = "d-300"

    sync_lead_to_hubspot(business=sample_business, lead_score_0_100=72, client=client)

    deal_call = client.create_deal.call_args
    props = deal_call.kwargs["properties"]
    assert props["dealname"] == "Joe's Pizza — Audit Outreach"
    assert props["dealstage"] == "stage-cold-test"
    assert props["pipeline"] == "pipe-test"
    assert props["deal_source"] == "Cold email"
    assert deal_call.kwargs["company_id"] == "co-100"
    assert deal_call.kwargs["contact_id"] == "c-200"


def test_sync_lead_returns_error_when_pipeline_env_missing(sample_business, monkeypatch):
    monkeypatch.delenv("HUBSPOT_PIPELINE_ID", raising=False)
    monkeypatch.delenv("HUBSPOT_STAGE_COLD_OUTREACH", raising=False)

    client = MagicMock()
    client.upsert_company.return_value = "co-100"
    client.upsert_contact.return_value = "c-200"

    result = sync_lead_to_hubspot(
        business=sample_business, lead_score_0_100=50, client=client
    )

    assert result.deal_id is None
    assert "HUBSPOT_PIPELINE_ID" in (result.error or "")
    client.create_deal.assert_not_called()


def test_sync_lead_returns_error_on_exception(sample_business):
    client = MagicMock()
    client.upsert_company.side_effect = RuntimeError("API down")

    result = sync_lead_to_hubspot(
        business=sample_business, lead_score_0_100=50, client=client
    )

    assert result.company_id is None
    assert result.contact_id is None
    assert result.deal_id is None
    assert "API down" in (result.error or "")


def test_normalize_score_to_1_10():
    from src.hubspot_sync import normalize_score_to_1_10

    assert normalize_score_to_1_10(0) == 1
    assert normalize_score_to_1_10(5) == 1
    assert normalize_score_to_1_10(50) == 5
    assert normalize_score_to_1_10(72) == 7
    assert normalize_score_to_1_10(100) == 10
    assert normalize_score_to_1_10(150) == 10  # clamp
    assert normalize_score_to_1_10(-10) == 1  # clamp


def test_maybe_sync_lead_skips_when_flag_disabled(sample_business, monkeypatch):
    """When HUBSPOT_SYNC_ENABLED is not 'true', maybe_sync_lead returns None."""
    from src.hubspot_sync import maybe_sync_lead

    monkeypatch.delenv("HUBSPOT_SYNC_ENABLED", raising=False)

    result = maybe_sync_lead(sample_business, lead_score_0_100=50)
    assert result is None


def test_maybe_sync_lead_calls_sync_when_enabled(sample_business, monkeypatch):
    """When HUBSPOT_SYNC_ENABLED=true, maybe_sync_lead invokes sync_lead_to_hubspot."""
    from unittest.mock import patch
    from src import hubspot_sync

    monkeypatch.setenv("HUBSPOT_SYNC_ENABLED", "true")

    expected = hubspot_sync.SyncResult("co-1", "c-1", "d-1", None)
    with patch.object(hubspot_sync, "sync_lead_to_hubspot", return_value=expected) as mock:
        result = hubspot_sync.maybe_sync_lead(sample_business, lead_score_0_100=50)

    assert result == expected
    mock.assert_called_once()
