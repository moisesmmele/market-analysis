import os
from types import SimpleNamespace
from pathlib import Path
import json

# Base Values
base_dir = Path(__file__).parent
data_dir = Path(base_dir).joinpath("data")

# Recursive namespace conversion from config dict
def to_namespace(d):
    return SimpleNamespace(**{
        k: to_namespace(v) if isinstance(v, dict) else v
        for k, v in d.items()
    })

# config with namespace mapping for access with dot notation
config = to_namespace({
    "data_dir": data_dir,
    "debug_dir": data_dir.joinpath("debug"),
    "topics": base_dir.joinpath("topics"),
    "mode": {"dev": False},
    "mappings": {
        "mappings_file": base_dir.joinpath("mappings.json"),
        "provider": "jobspy",
        "platform": "linkedin"
    },
    "database": {
        "file": data_dir.joinpath("database.db"),
        "schema": base_dir.joinpath("schema.sql")
    }
})
