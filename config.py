from pathlib import Path
from types import SimpleNamespace
import os

# Recursive namespace conversion from config dict
def to_namespace(d):
    return SimpleNamespace(**{
        k: to_namespace(v) if isinstance(v, dict) else v
        for k, v in d.items()
    })

data_dir = Path(os.path.dirname(__file__)).joinpath("data")

config = to_namespace({
    "data_dir": data_dir,
    "database": {
        "file": data_dir.joinpath("database.db"),
        "schema": data_dir.joinpath("schema.sql")
    }
})
