from pathlib import Path
from typing import Any
import json

from app import config

class MappingsLoader:
    _CACHED_MAPPINGS: dict[str, Any] = None

    @classmethod
    def load_mappings(cls) -> None:

        # For now, we pass a single file - in the future,
        # we should pass a mappings directory and iterate over
        # each file with glob - the mappings should have a
        # strict schema, allowing us to automatically pick the
        # appropriate mappings based on session data
        _filename: Path =  config.resources.mappings
        with open(_filename, "r") as file:
            mappings = json.load(file)
            if not mappings:
                raise Exception(f"Mappings were not found for '{_filename}'")

        # needs validation - for now that's what it is
        # provider and platform could be passed as parameters
        # this way we could platform/provider specific mappings
        # on demand, instead of checking provider/platform name
        # from config definitions

        canonical_mappings = mappings.get("canonical", {})
        if not canonical_mappings:
            raise Exception(f"Mappings were not found for canonical at: {_filename}")

        provider = config.mappings.provider
        provider_mappings = mappings.get("providers", {}).get(provider)
        if not provider_mappings:
            raise Exception(f"Mappings were not found for '{provider}' at: {_filename}")

        platform = config.mappings.platform
        platform_mappings = mappings.get("platforms", {}).get(platform)
        if not platform_mappings:
            raise Exception(f"Mappings were not found for '{platform}' at: {_filename}")

        cls._CACHED_MAPPINGS = {
            "canonical": canonical_mappings,
            "provider": provider_mappings,
            "platform": platform_mappings
        }

    @classmethod
    def get_mappings(cls, mappings: list[str] = None) -> dict[str, Any]:
        if not cls._CACHED_MAPPINGS:
            cls.load_mappings()

        if mappings:
            return {
                mapping: cls._CACHED_MAPPINGS[mapping]
                for mapping
                in mappings}

        return cls._CACHED_MAPPINGS