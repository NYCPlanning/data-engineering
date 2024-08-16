from pathlib import Path

from dcpy.utils import postgres
from dcpy.lifecycle.builds import metadata

PRODUCT_PATH = Path(__file__).resolve().parent.parent

RECIPE_PATH = PRODUCT_PATH / "recipe.yml"

SQL_QUERY_DIR = PRODUCT_PATH / "sql"
OUTPUT_DIR = PRODUCT_PATH / "output"

PRODUCT_S3_NAME = "db-template"

BUILD_NAME = metadata.build_name()

PG_CLIENT = postgres.PostgresClient(
    schema=BUILD_NAME,
)
