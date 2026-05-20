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


def test_upsert_company_creates_when_not_found(client):
    """upsert_company calls find first; creates when not found."""
    with (
        patch.object(client, "find_company_by_domain", return_value=None),
        patch.object(client, "create_company", return_value="new-id") as mock_create,
    ):
        company_id = client.upsert_company(
            domain="newone.com", properties={"name": "New Co", "domain": "newone.com"}
        )

    assert company_id == "new-id"
    mock_create.assert_called_once()


def test_upsert_company_returns_existing_id_when_found(client):
    """upsert_company returns existing ID and does NOT call create."""
    with (
        patch.object(client, "find_company_by_domain", return_value="existing-id"),
        patch.object(client, "create_company") as mock_create,
    ):
        company_id = client.upsert_company(
            domain="existing.com", properties={"name": "Existing Co"}
        )

    assert company_id == "existing-id"
    mock_create.assert_not_called()


def test_create_contact_returns_id(client):
    fake_response = MagicMock()
    fake_response.id = "c-123"
    with patch.object(
        client._contacts_api, "create", return_value=fake_response
    ) as mock_create:
        new_id = client.create_contact(
            properties={"email": "joe@joespizza.com", "firstname": "Joe", "lastname": "P"}
        )
    assert new_id == "c-123"
    mock_create.assert_called_once()


def test_find_contact_by_email_returns_id_when_found(client):
    found = MagicMock()
    found.id = "c-999"
    response = MagicMock()
    response.results = [found]
    with patch.object(
        client._contacts_search_api, "do_search", return_value=response
    ):
        assert client.find_contact_by_email("joe@joespizza.com") == "c-999"


def test_find_contact_by_email_returns_none_when_missing(client):
    response = MagicMock()
    response.results = []
    with patch.object(
        client._contacts_search_api, "do_search", return_value=response
    ):
        assert client.find_contact_by_email("missing@nowhere.com") is None


def test_upsert_contact_creates_when_not_found(client):
    with (
        patch.object(client, "find_contact_by_email", return_value=None),
        patch.object(client, "create_contact", return_value="c-new"),
    ):
        assert (
            client.upsert_contact(
                email="new@joespizza.com", properties={"email": "new@joespizza.com"}
            )
            == "c-new"
        )


def test_upsert_contact_returns_existing_id_when_found(client):
    with (
        patch.object(client, "find_contact_by_email", return_value="c-existing"),
        patch.object(client, "create_contact") as mock_create,
    ):
        result = client.upsert_contact(
            email="existing@joespizza.com", properties={"email": "existing@joespizza.com"}
        )
        assert result == "c-existing"
        mock_create.assert_not_called()
