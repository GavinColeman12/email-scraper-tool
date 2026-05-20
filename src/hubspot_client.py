"""Low-level wrapper around the HubSpot REST API.

Knows nothing about restaurants or business dicts — pure CRUD on Companies,
Contacts, Deals, and associations. Designed to be mock-friendly for unit tests.
"""

from __future__ import annotations

from typing import Optional

from hubspot import HubSpot
from hubspot.crm.companies.exceptions import ApiException as CompanyApiException
from hubspot.crm.contacts.exceptions import ApiException as ContactApiException
from hubspot.crm.deals.exceptions import ApiException as DealApiException
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)
from hubspot.crm.companies import (
    Filter,
    FilterGroup,
    PublicObjectSearchRequest,
    SimplePublicObjectInputForCreate as CompanyCreate,
)
from hubspot.crm.contacts import SimplePublicObjectInputForCreate as ContactCreate
from hubspot.crm.contacts import (
    Filter as ContactFilter,
    FilterGroup as ContactFilterGroup,
    PublicObjectSearchRequest as ContactSearchRequest,
)
from hubspot.crm.deals import (
    SimplePublicObjectInputForCreate as DealCreate,
    PublicAssociationsForObject,
    AssociationSpec,
)


def _is_rate_limit(exc: BaseException) -> bool:
    return (
        isinstance(exc, (CompanyApiException, ContactApiException, DealApiException))
        and getattr(exc, "status", None) == 429
    )


_retry_on_rate_limit = retry(
    retry=retry_if_exception(_is_rate_limit),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)


class HubSpotClient:
    def __init__(self, access_token: str) -> None:
        self._api = HubSpot(access_token=access_token)
        # Cache sub-clients so patch.object targets are stable across calls.
        # The HubSpot SDK builds new objects on every property access, so
        # storing references here ensures the same instance is patched in tests.
        self._companies_api = self._api.crm.companies.basic_api
        self._companies_search_api = self._api.crm.companies.search_api
        self._contacts_api = self._api.crm.contacts.basic_api
        self._contacts_search_api = self._api.crm.contacts.search_api
        self._deals_api = self._api.crm.deals.basic_api
        self._associations_v4_api = self._api.crm.associations.v4.basic_api

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

    @_retry_on_rate_limit
    def create_company(self, properties: dict) -> str:
        payload = CompanyCreate(properties=properties)
        result = self._companies_api.create(
            simple_public_object_input_for_create=payload
        )
        return result.id

    def upsert_company(self, domain: str, properties: dict) -> str:
        existing = self.find_company_by_domain(domain)
        if existing:
            return existing
        return self.create_company(properties)

    @_retry_on_rate_limit
    def create_contact(self, properties: dict) -> str:
        payload = ContactCreate(properties=properties)
        result = self._contacts_api.create(
            simple_public_object_input_for_create=payload
        )
        return result.id

    def find_contact_by_email(self, email: str) -> Optional[str]:
        request = ContactSearchRequest(
            filter_groups=[
                ContactFilterGroup(
                    filters=[ContactFilter(property_name="email", operator="EQ", value=email)]
                )
            ],
            properties=["email"],
            limit=1,
        )
        response = self._contacts_search_api.do_search(
            public_object_search_request=request
        )
        if response.results:
            return response.results[0].id
        return None

    def upsert_contact(self, email: str, properties: dict) -> str:
        existing = self.find_contact_by_email(email)
        if existing:
            return existing
        return self.create_contact(properties)

    @_retry_on_rate_limit
    def create_deal(
        self,
        properties: dict,
        company_id: Optional[str] = None,
        contact_id: Optional[str] = None,
    ) -> str:
        associations: list[PublicAssociationsForObject] = []
        if company_id:
            associations.append(
                PublicAssociationsForObject(
                    to={"id": company_id},
                    types=[
                        AssociationSpec(
                            association_category="HUBSPOT_DEFINED",
                            association_type_id=5,  # deal_to_company
                        )
                    ],
                )
            )
        if contact_id:
            associations.append(
                PublicAssociationsForObject(
                    to={"id": contact_id},
                    types=[
                        AssociationSpec(
                            association_category="HUBSPOT_DEFINED",
                            association_type_id=3,  # deal_to_contact
                        )
                    ],
                )
            )
        payload = DealCreate(properties=properties, associations=associations)
        result = self._deals_api.create(
            simple_public_object_input_for_create=payload
        )
        return result.id

    def associate_contact_to_company(self, contact_id: str, company_id: str) -> None:
        from hubspot.crm.associations.v4 import AssociationSpec as V4Spec

        self._associations_v4_api.create(
            object_type="contacts",
            object_id=contact_id,
            to_object_type="companies",
            to_object_id=company_id,
            association_spec=[
                V4Spec(association_category="HUBSPOT_DEFINED", association_type_id=1)
            ],
        )
