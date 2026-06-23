from pathlib import Path

from dcpy.configuration import BUILD_NAME
from dcpy.lifecycle import config as lifecycle_config
from dcpy.lifecycle.builds import get_recipe_lock, metadata
from dcpy.utils import postgres

PRODUCT_PATH = Path(__file__).resolve().parent.parent
PRODUCT_NAME = "template"

RECIPE_PATH = PRODUCT_PATH / "recipe.yml"

SQL_QUERY_DIR = PRODUCT_PATH / "sql"

BUILD_ENGINE_SCHEMA = metadata.build_name(BUILD_NAME)


def get_output_dir() -> Path:
    """Get the build output directory path from dcpy.

    Returns:
        Path to DCPY_LIFECYCLE_DATA_DIR/builds/build/template/{version}
    """
    recipe = get_recipe_lock(PRODUCT_PATH)
    if not recipe.version:
        raise ValueError("Recipe version not set")
    return lifecycle_config.get_build_dir(PRODUCT_NAME, recipe.version)


# For backwards compatibility, try to get OUTPUT_DIR
# If recipe.lock.yml doesn't exist yet, fall back to product path
try:
    OUTPUT_DIR = get_output_dir()
except Exception:
    # Fallback for when recipe hasn't been locked yet
    OUTPUT_DIR = PRODUCT_PATH / "output"

PG_CLIENT = postgres.PostgresClient(
    schema=BUILD_ENGINE_SCHEMA,
)
