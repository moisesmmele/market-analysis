from mappings_loader import MappingsLoader
from dynamic_listing import DynamicListing
from typing import Any
import json

class DynamicListingFactory:

    @staticmethod
    def create(listing_id: int, raw_data: str) -> DynamicListing:

        mappings: dict[str, dict[str, Any]] = MappingsLoader.get_mappings()
        provider_map: dict[str, Any] = mappings["provider"]
        platform_map: dict[str, Any] = mappings["platform"]

        parsed_listing_data: dict[str, str] = json.loads(raw_data)

        mapped_data = {"id": str(listing_id)}

        valid_keys = set(DynamicListing.__annotations__.keys())
        valid_keys.remove('id')

        for canonical_field in valid_keys:
            # get provider-specific field name
            provider_field = provider_map.get("fields").get(canonical_field)
            if provider_field is None:
                continue

            # get the value from parsed data
            value = parsed_listing_data.get(provider_field)
            if value is None:
                continue

            #default to version value as the canonical
            mapped_data[canonical_field] = value

            # check if there's platform-specific data format mappings for that field
            platform_mappings_for_field: dict[str, list[str]] = platform_map.get(canonical_field)
            if platform_mappings_for_field:
                for canonical_term, platform_terms in platform_mappings_for_field.items():
                    if value in platform_terms:
                        mapped_data[canonical_field] = canonical_term
                        break

        return DynamicListing(**mapped_data)