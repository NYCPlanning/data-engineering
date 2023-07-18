import os
import sys
from pathlib import Path
from dotenv import dotenv_values


_module_top_path = Path(__file__).resolve().parent
_product_path = _module_top_path.parent
_proj_root = _product_path.parent

# Make `dcpy` available
sys.path.append(str(_proj_root))

_config = dotenv_values(_product_path / "version.env")

VERSION_PREV = _config["VERSION_PREV"]
SQL_PATH = _module_top_path / "sql"
BASH_PATH = _module_top_path / "bash"
CACHE_PATH = _product_path / ".cache"
BASE_URL = "https://nyc3.digitaloceanspaces.com/edm-recipes/datasets"

BUILD_ENGINE = os.environ.get("BUILD_ENGINE")
assert BUILD_ENGINE
