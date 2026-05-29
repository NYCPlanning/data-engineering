import shutil
from pathlib import Path

from dcpy.lifecycle.builds import (
    get_build_metadata_path,
    get_recipe_lock,
    get_recipe_lock_path,
)
from dcpy.lifecycle.config import get_build_dir
from dcpy.utils.logging import logger

PRODUCT_PATH = Path(__file__).parent
PRODUCT_NAME = "edde"


class BuildNotPlannedError(Exception):
    """Raised when the recipe has not been planned (recipe.lock.yml doesn't exist)."""

    pass


class BuildDataNotLoadedError(Exception):
    """Raised when the build metadata hasn't been created (build_metadata.json doesn't exist)."""

    pass


def get_build_output_dir() -> Path:
    """
    Get the build output directory path for build data.

    Returns DCPY_LIFECYCLE_DATA_DIR/build/edde/{version} where version is
    retrieved from recipe.lock.yml vars.VERSION

    Returns:
        Path to the build output directory

    Raises:
        BuildNotPlannedError: If recipe.lock.yml doesn't exist
        BuildDataNotLoadedError: If build_metadata.json doesn't exist
        ValueError: If VERSION not found in recipe.lock.yml vars
    """
    # Check that recipe has been planned
    recipe_lock_path = get_recipe_lock_path(PRODUCT_PATH)
    if not recipe_lock_path.exists():
        raise BuildNotPlannedError(
            f"Recipe has not been planned. Please run 'bd plan' first.\n"
            f"Expected recipe.lock.yml at: {recipe_lock_path}"
        )

    # Check that build metadata exists (data has been loaded)
    build_metadata_path = get_build_metadata_path(PRODUCT_PATH)
    if not build_metadata_path.exists():
        raise BuildDataNotLoadedError(
            f"Build data has not been loaded. Please run 'bd load' first.\n"
            f"Expected build_metadata.json at: {build_metadata_path}"
        )

    # Get version from recipe lockfile
    recipe_lock = get_recipe_lock(PRODUCT_PATH)
    if not recipe_lock.vars or "VERSION" not in recipe_lock.vars:
        raise ValueError(
            f"VERSION not found in recipe lockfile vars at {get_recipe_lock_path(PRODUCT_PATH)}"
        )
    version = recipe_lock.vars["VERSION"]

    build_output_dir = get_build_dir(PRODUCT_NAME, version)
    logger.info(f"Build output directory: {build_output_dir}")
    return build_output_dir


def clean_build_output_dir(categories: list[str] | None = None) -> None:
    """
    Clean category subfolders in the build output data directory before re-running a build.

    Args:
        categories: List of category names to clean (e.g., ["demographics", "economics"]).
                   If None, removes the data directory in the build output directory.

    Raises:
        BuildNotPlannedError: If recipe.lock.yml doesn't exist
        BuildDataNotLoadedError: If build_metadata.json doesn't exist
        ValueError: If VERSION not found in recipe.lock.yml vars
    """
    build_output_dir = get_build_output_dir()
    data_dir = build_output_dir / "data"

    if categories:
        logger.info(
            f"Cleaning categories {categories} in build output data directory: {data_dir}"
        )
    else:
        logger.info(f"Cleaning data directory in build output directory: {data_dir}")

    if not data_dir.exists():
        logger.info(f"Data directory does not exist yet: {data_dir}")
        return

    items_removed = 0

    if categories:
        # Only remove specific category directories within data/
        for category in categories:
            category_path = data_dir / category
            if category_path.exists() and category_path.is_dir():
                logger.info(f"Removing directory: {category_path}")
                shutil.rmtree(category_path)
                items_removed += 1
            elif category_path.exists():
                logger.info(f"Removing file: {category_path}")
                category_path.unlink()
                items_removed += 1
    else:
        # Remove entire data directory
        if data_dir.exists() and data_dir.is_dir():
            logger.info(f"Removing data directory: {data_dir}")
            shutil.rmtree(data_dir)
            items_removed += 1

    if items_removed == 0:
        logger.info("No items to remove")
    else:
        logger.info(f"Removed {items_removed} items")
