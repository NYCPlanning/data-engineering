import os
from pathlib import Path

_module_top_path = Path(__file__).resolve().parent
PRODUCT_PATH = _module_top_path.parent

SQL_PATH = _module_top_path / "sql"
BASH_PATH = _module_top_path / "bash"
CACHE_PATH = PRODUCT_PATH / ".cache"
BASE_URL = "https://nyc3.digitaloceanspaces.com/edm-recipes/datasets"

BUILD_NAME = os.environ["BUILD_ENGINE_SCHEMA"]

BUILD_ENGINE = os.environ["BUILD_ENGINE"]
assert BUILD_ENGINE
