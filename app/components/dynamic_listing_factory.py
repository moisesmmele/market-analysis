from app.entities import DynamicListing
from app.loaders import MappingsLoader
from typing import Any
import json

class DynamicListingFactory:

    @staticmethod
    def create(listing_id: int, raw_data: str) -> DynamicListing|None:

        mappings: dict[str, dict[str, Any]] = MappingsLoader.get_mappings()
        if not mappings:
            raise Exception("Mappings were not loaded")

        provider_map: dict[str, Any] = mappings["provider"]
        platform_map: dict[str, Any] = mappings["platform"]

        parsed_listing_data: dict[str, str] = json.loads(raw_data)

        mapped_data = {"id": str(listing_id)}

        valid_keys = set(DynamicListing.__annotations__.keys())
        valid_keys.remove('id')

        for canonical_field in valid_keys:
            # get provider-specific field name
            provider_field_mappings = provider_map.get("fields")
            if not provider_field_mappings:
                raise Exception("Could not load mappings")

            provider_field = provider_field_mappings.get(canonical_field)
            if not provider_field:
                continue

            # get the value from parsed data
            value: str|int = parsed_listing_data.get(provider_field)
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