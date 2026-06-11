import json
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


def get_edde_paths() -> tuple[Path, Path]:
    """Get paths for old (loaded EDDE) and new (current build) data.

    Returns:
        Tuple of (old_edde_path, new_build_path)
        - old_edde_path: Path to loaded EDDE dataset from previous version
        - new_build_path: Path to current build data directory
    """
    # Load build metadata
    build_metadata_path = get_build_metadata_path(PRODUCT_PATH)
    if not build_metadata_path.exists():
        raise FileNotFoundError(
            f"build_metadata.json not found at {build_metadata_path}. "
            "Please run 'bd load' first."
        )

    with open(build_metadata_path, "r") as f:
        build_metadata = json.load(f)

    # Get current version from recipe
    current_version = build_metadata["recipe"]["vars"]["BUILD_ENV_EDDE_VERSION"]

    # Get old EDDE path from loaded dataset
    if "load_result" not in build_metadata:
        raise ValueError(
            "load_result not found in build_metadata.json. "
            "Please run 'bd load' to load the recipe datasets first."
        )

    load_result = build_metadata["load_result"]
    if "datasets" not in load_result or "edde" not in load_result["datasets"]:
        raise ValueError(
            "EDDE dataset not found in load_result. "
            "Please ensure the recipe includes the 'edde' dataset and run 'bd load' first."
        )

    # Get the first (and should be only) version of the edde dataset
    edde_versions = load_result["datasets"]["edde"]
    if not edde_versions:
        raise ValueError(
            "No EDDE dataset versions found in load_result. "
            "Please ensure the recipe includes the 'edde' dataset and run 'bd load' first."
        )

    # Get the first version's destination path
    edde_version_key = list(edde_versions.keys())[0]
    edde_data = edde_versions[edde_version_key]
    # The loaded EDDE dataset has a "data" subdirectory containing the actual CSVs
    old_edde_path = Path(edde_data["destination"]) / "data"

    # Get new build path from current build output directory
    new_build_path = get_build_dir(PRODUCT_NAME, current_version) / "data"

    return old_edde_path, new_build_path


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
    if not recipe_lock.vars or "BUILD_ENV_EDDE_VERSION" not in recipe_lock.vars:
        raise ValueError(
            f"BUILD_ENV_EDDE_VERSION not found in recipe lockfile vars at {get_recipe_lock_path(PRODUCT_PATH)}"
        )
    version = recipe_lock.vars["BUILD_ENV_EDDE_VERSION"]

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
