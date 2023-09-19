import os
import sys
from pathlib import Path
from dotenv import load_dotenv


_module_top_path = Path(__file__).resolve().parent
PRODUCT_PATH = _module_top_path.parent
_proj_root = PRODUCT_PATH.parent.parent

# Make `dcpy` available
sys.path.append(str(_proj_root))

load_dotenv(PRODUCT_PATH / "version.env")

SQL_PATH = _module_top_path / "sql"
BASH_PATH = _module_top_path / "bash"
CACHE_PATH = PRODUCT_PATH / ".cache"
BASE_URL = "https://nyc3.digitaloceanspaces.com/edm-recipes/datasets"

BUILD_ENGINE = os.environ.get("BUILD_ENGINE", "")
assert BUILD_ENGINE
