"""Low-level wrapper around the HubSpot REST API.

Knows nothing about restaurants or business dicts — pure CRUD on Companies,
Contacts, Deals, and associations. Designed to be mock-friendly for unit tests.
"""

from __future__ import annotations

from typing import Optional

from hubspot import HubSpot
from hubspot.crm.companies import (
    Filter,
    FilterGroup,
    PublicObjectSearchRequest,
    SimplePublicObjectInputForCreate as CompanyCreate,
)


class HubSpotClient:
    def __init__(self, access_token: str) -> None:
        self._api = HubSpot(access_token=access_token)
        # Cache sub-clients so patch.object targets are stable across calls.
        # The HubSpot SDK builds new objects on every property access, so
        # storing references here ensures the same instance is patched in tests.
        self._companies_api = self._api.crm.companies.basic_api
        self._companies_search_api = self._api.crm.companies.search_api

    def find_company_by_domain(self, domain: str) -> Optional[str]:
        request = PublicObjectSearchRequest(
            filter_groups=[
                FilterGroup(filters=[Filter(property_name="domain", operator="EQ", value=domain)])
            ],
            properties=["domain"],
            limit=1,
        )
        response = self._companies_search_api.do_search(
            public_object_search_request=request
        )
        if response.results:
            return response.results[0].id
        return None

    def create_company(self, properties: dict) -> str:
        payload = CompanyCreate(properties=properties)
        result = self._companies_api.create(
            simple_public_object_input_for_create=payload
        )
        return result.id
