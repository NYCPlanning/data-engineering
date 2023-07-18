import os
from pathlib import Path
from dotenv import dotenv_values


_module_top_path = Path(__file__).resolve().parent
_config = dotenv_values(_module_top_path.parent / "version.env")

VERSION_PREV = _config["VERSION_PREV"]

SQL_PATH = _module_top_path / "sql"
BASH_PATH = _module_top_path / "bash"
CACHE_PATH = _module_top_path.parent / ".cache"

BASE_URL = "https://nyc3.digitaloceanspaces.com/edm-recipes/datasets"

BUILD_ENGINE = os.environ.get("BUILD_ENGINE")
assert BUILD_ENGINE
