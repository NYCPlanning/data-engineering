import os
from pathlib import Path

from dcpy.lifecycle import config

# Build configuration constants
BUILD_ARTIFACT_DIRS = ["target", "attachments", "dataset_files"]
BUILD_STAGE_KEY = "builds.build"  # Recipe stage_config key for build commands


def get_recipe_path(product_path: Path, recipe_name: str | None = None) -> Path:
    """Get the path to a recipe file for a product.

    Args:
        product_path: Path to the product directory
        recipe_name: Name of the recipe file (e.g., "my-recipe.yml").
                    If None, defaults to "recipe.yml"

    Returns:
        Path to the recipe file
    """
    if recipe_name is None:
        recipe_name = "recipe"
    return product_path / f"{recipe_name}.yml"


def get_recipe_lock_path(product_path: Path, recipe_name: str | None = None) -> Path:
    """Get the path to a recipe lockfile in the product directory.

    Note: During planning, lockfiles are written to the plan directory using get_plan_dir().
    This function is for accessing lockfiles that have been copied to the product directory,
    typically for local builds or legacy workflows.

    Args:
        product_path: Path to the product directory
        recipe_name: Name of the recipe file (e.g., "my-recipe.yml").
                    If None, defaults to "recipe.yml"

    Returns:
        Path to the recipe lockfile (e.g., "recipe.lock.yml" or "my-recipe.lock.yml")
    """
    recipe_path = get_recipe_path(product_path, recipe_name)
    # Insert .lock before .yml extension
    return recipe_path.parent / f"{recipe_path.stem}.lock.yml"


def get_build_metadata_path(product_path: Path) -> Path:
    """Get the path to the build metadata file for a product.

    Args:
        product_path: Path to the product directory

    Returns:
        Path to the build_metadata.json file
    """
    return product_path / "build_metadata.json"


def get_build_output_dir(product: str, version: str) -> Path:
    """Directory a build's artifacts (duckdb file, exports) live in.

    BUILD_ENV_OUTPUT_DIR overrides the standard {product}/{version} build dir. Load, build,
    and export run as separate processes, so they all resolve it here to agree.
    """
    if "BUILD_ENV_OUTPUT_DIR" in os.environ:
        return Path(os.environ["BUILD_ENV_OUTPUT_DIR"])
    return config.get_build_dir(product, version)


def get_duckdb_path(product: str, version: str) -> Path:
    return get_build_output_dir(product, version) / f"{product}_{version}.duckdb"
