"""Low-level wrapper around the HubSpot REST API.

Knows nothing about restaurants or business dicts — pure CRUD on Companies,
Contacts, Deals, and associations. Designed to be mock-friendly for unit tests.
"""

from __future__ import annotations

from typing import Optional

from hubspot import HubSpot
from hubspot.crm.companies import SimplePublicObjectInputForCreate as CompanyCreate


class HubSpotClient:
    def __init__(self, access_token: str) -> None:
        self._api = HubSpot(access_token=access_token)
        # Cache sub-clients so patch.object targets are stable across calls.
        # The HubSpot SDK builds new objects on every property access, so
        # storing references here ensures the same instance is patched in tests.
        self._companies_api = self._api.crm.companies.basic_api

    def create_company(self, properties: dict) -> str:
        payload = CompanyCreate(properties=properties)
        result = self._companies_api.create(
            simple_public_object_input_for_create=payload
        )
        return result.id
