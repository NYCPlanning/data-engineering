import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

_product_path = Path(__file__).resolve().parent.parent
_proj_root = _product_path.parent.parent

# Make `dcpy` available
sys.path.append(str(_proj_root))

RECIPE_PATH = _product_path / "recipe.yml"
RECIPE_LOCK_PATH = _product_path / "recipe.lock.yml"

SQL_QUERY_DIR = _product_path / "sql"
