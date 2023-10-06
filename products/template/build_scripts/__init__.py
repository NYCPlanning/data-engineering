import sys
from pathlib import Path
from dotenv import load_dotenv

from dcpy.utils import git, postgres

load_dotenv()

_product_path = Path(__file__).resolve().parent.parent

PRODUCT_S3_NAME = "db-template"

RECIPE_PATH = _product_path / "recipe.yml"

SQL_QUERY_DIR = _product_path / "sql"
OUTPUT_DIR = _product_path / "output"

BUILD_NAME = git.run_name()

PG_CLIENT = postgres.PostgresClient(
    schema=BUILD_NAME,
)
