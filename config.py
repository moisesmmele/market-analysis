from pathlib import Path
from types import SimpleNamespace
import os

# Recursive namespace conversion from config dict
def to_namespace(d):
    return SimpleNamespace(**{
        k: to_namespace(v) if isinstance(v, dict) else v
        for k, v in d.items()
    })

base_dir = Path(__file__).parent
data_dir = Path(base_dir).joinpath("data")

config = to_namespace({
    "data_dir": data_dir,
    "keywords": base_dir.joinpath("keywords"),
    "database": {
        "file": data_dir.joinpath("database.db"),
        "schema": base_dir.joinpath("schema.sql")
    }
})
