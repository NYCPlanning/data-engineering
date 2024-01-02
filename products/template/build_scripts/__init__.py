from pathlib import Path

from dcpy.utils import postgres
from dcpy.builds import metadata


_product_path = Path(__file__).resolve().parent.parent

PRODUCT_S3_NAME = "db-template"

RECIPE_PATH = _product_path / "recipe.yml"

SQL_QUERY_DIR = _product_path / "sql"
OUTPUT_DIR = _product_path / "output"

BUILD_NAME = metadata.build_name()

PG_CLIENT = postgres.PostgresClient(
    schema=BUILD_NAME,
)
