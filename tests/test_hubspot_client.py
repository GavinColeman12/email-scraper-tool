"""Tests for the low-level HubSpot REST client wrapper."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.hubspot_client import HubSpotClient


@pytest.fixture
def client():
    """Return a HubSpotClient with a fake access token."""
    return HubSpotClient(access_token="test-token")


def test_create_company_returns_id(client):
    """create_company should POST to companies endpoint and return the new ID."""
    fake_response = MagicMock()
    fake_response.id = "12345"

    with patch.object(
        client._companies_api, "create", return_value=fake_response
    ) as mock_create:
        new_id = client.create_company(
            properties={
                "name": "Joe's Pizza",
                "domain": "joespizza.com",
                "cuisine_type": "Pizza",
            }
        )

    assert new_id == "12345"
    mock_create.assert_called_once()
    call_args = mock_create.call_args.kwargs
    payload = call_args["simple_public_object_input_for_create"]
    assert payload.properties["name"] == "Joe's Pizza"
    assert payload.properties["domain"] == "joespizza.com"
    assert payload.properties["cuisine_type"] == "Pizza"


def test_find_company_by_domain_returns_id_when_found(client):
    """find_company_by_domain returns the matching company ID, or None if not found."""
    found = MagicMock()
    found.id = "99999"
    response = MagicMock()
    response.results = [found]

    with patch.object(
        client._companies_search_api, "do_search", return_value=response
    ) as mock_search:
        company_id = client.find_company_by_domain("joespizza.com")

    assert company_id == "99999"
    mock_search.assert_called_once()


def test_find_company_by_domain_returns_none_when_missing(client):
    """find_company_by_domain returns None if no company matches the domain."""
    response = MagicMock()
    response.results = []

    with patch.object(
        client._companies_search_api, "do_search", return_value=response
    ):
        company_id = client.find_company_by_domain("nonexistent.com")

    assert company_id is None
