from pathlib import Path

from dcpy.lifecycle.builds import metadata
from dcpy.models.connectors.edm.publishing import BuildKey
from dcpy.utils import postgres

PRODUCT_PATH = Path(__file__).resolve().parent.parent

RECIPE_PATH = PRODUCT_PATH / "recipe.yml"

SQL_QUERY_DIR = PRODUCT_PATH / "sql"
OUTPUT_DIR = PRODUCT_PATH / "output"

PRODUCT_S3_NAME = "db-template"
BUILD_NAME = metadata.build_name()
BUILD_KEY = BuildKey(product=PRODUCT_S3_NAME, build=BUILD_NAME)

PG_CLIENT = postgres.PostgresClient(
    schema=BUILD_NAME,
)
