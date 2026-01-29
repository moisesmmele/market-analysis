from config import config
from typing import Any
import json

class MappingsLoader:
    _cached_mappings: dict[str, Any] = None

    @classmethod
    def load_mappings(cls) -> None:
        with open(config.mappings.mappings_file, "r") as file:
            mappings = json.load(file)
        #needs validation - for now that's what it is
        cls._cached_mappings = {
            "canonical": mappings.get("canonical", {}),
            "provider": mappings.get("providers", {}).get(config.mappings.provider),
            "platform": mappings.get("platforms", {}).get(config.mappings.platform)
        }

    @classmethod
    def get_mappings(cls, mappings: list[str] = None) -> dict[str, Any]:
        if not cls._cached_mappings:
            cls.load_mappings()

        if mappings:
            return {mapping: cls._cached_mappings[mapping] for mapping in mappings}

        return cls._cached_mappings