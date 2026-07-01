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
PRODUCT_NAME = "template"


class BuildNotPlannedError(Exception):
    """Raised when the recipe has not been planned (recipe.lock.yml doesn't exist)."""

    pass


class BuildDataNotLoadedError(Exception):
    """Raised when the build metadata hasn't been created (build_metadata.json doesn't exist)."""

    pass


def get_build_output_dir() -> Path:
    """
    Get the build output directory path for build data.

    Returns DCPY_LIFECYCLE_DATA_DIR/build/template/{version} where version is
    retrieved from recipe.lock.yml version field.

    Returns:
        Path to the build output directory

    Raises:
        BuildNotPlannedError: If recipe.lock.yml doesn't exist
        BuildDataNotLoadedError: If build_metadata.json doesn't exist
        ValueError: If version not found in recipe.lock.yml
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
    if not recipe_lock.version:
        raise ValueError(
            f"version not found in recipe lockfile at {get_recipe_lock_path(PRODUCT_PATH)}"
        )
    version = recipe_lock.version

    build_output_dir = get_build_dir(PRODUCT_NAME, version)
    logger.info(f"Build output directory: {build_output_dir}")
    return build_output_dir


def clean_build_output_dir() -> None:
    """
    Clean the build output directory before re-running a build.

    Raises:
        BuildNotPlannedError: If recipe.lock.yml doesn't exist
        BuildDataNotLoadedError: If build_metadata.json doesn't exist
        ValueError: If version not found in recipe.lock.yml
    """
    build_output_dir = get_build_output_dir()

    logger.info(f"Cleaning build output directory: {build_output_dir}")

    if not build_output_dir.exists():
        logger.info(f"Build output directory does not exist yet: {build_output_dir}")
        return

    # Remove entire build output directory
    if build_output_dir.exists() and build_output_dir.is_dir():
        logger.info(f"Removing build output directory: {build_output_dir}")
        shutil.rmtree(build_output_dir)
        logger.info("Build output directory removed")
    else:
        logger.info("No build output directory to remove")
