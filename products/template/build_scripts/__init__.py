from pathlib import Path

from dcpy.configuration import BUILD_NAME
from dcpy.lifecycle.builds import metadata
from dcpy.utils import postgres

PRODUCT_PATH = Path(__file__).resolve().parent.parent

RECIPE_PATH = PRODUCT_PATH / "recipe.yml"

SQL_QUERY_DIR = PRODUCT_PATH / "sql"
OUTPUT_DIR = PRODUCT_PATH / "output"

BUILD_ENGINE_SCHEMA = metadata.build_name(BUILD_NAME)

PG_CLIENT = postgres.PostgresClient(
    schema=BUILD_ENGINE_SCHEMA,
)
