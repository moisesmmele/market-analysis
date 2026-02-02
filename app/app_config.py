from types import SimpleNamespace
from pathlib import Path

# Base Values
project_root = Path(__file__).resolve().parent.parent

app_root: Path = project_root.joinpath("app")

resources_dir: Path = project_root.joinpath("resources")
data_dir: Path = project_root.joinpath(".data")
debug_dir: Path = project_root.joinpath(".debug")

# Recursive namespace conversion from config dict
def to_namespace(d):
    return SimpleNamespace(**{
        k: to_namespace(v) if isinstance(v, dict) else v
        for k, v in d.items()
    })

# config with namespace mapping for access with dot notation
config = to_namespace({
    "mode": {"dev": False},
    "database": {
        "file": data_dir.joinpath("database.db"),
        "schema": app_root.joinpath("persistence").joinpath("schema.sql")
    },
    "dir": {
        "debug": debug_dir,
        "data": data_dir,
        "resources": resources_dir,
        "app": app_root
    },
    "resources" :{
        "topics": resources_dir.joinpath("topics"),
        # mappings should be a dir, and we should have n mappings for n scenarios
        # for now lets stick with just one file mappings.json
        "mappings": resources_dir.joinpath("mappings").joinpath("mappings.json"),
    },
    # these are provider/platform specific names - should not have to exist
    # we should pick provider and platform dynamically from session meta (or specific fields?)
    "mappings": {
        "provider": "jobspy",
        "platform": "linkedin"
    }
})