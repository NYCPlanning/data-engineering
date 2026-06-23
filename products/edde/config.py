import shutil
from pathlib import Path

from dcpy.lifecycle.builds import (
    get_recipe_lock,
    get_recipe_lock_path,
)
from dcpy.lifecycle.builds.load import get_build_metadata
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
    # Get current build info from recipe.lock.yml
    recipe_lock = get_recipe_lock(PRODUCT_PATH)
    if not recipe_lock.build_name:
        raise ValueError("build_name not found in recipe.lock.yml")
    current_build_name = recipe_lock.build_name

    # Load build metadata for old EDDE path
    build_metadata = get_build_metadata(PRODUCT_PATH)

    # Get old EDDE path from loaded dataset
    if not build_metadata.load_result:
        raise ValueError(
            "load_result not found in build_metadata.json. "
            "Please run 'bd load' to load the recipe datasets first."
        )

    load_result = build_metadata.load_result
    if not load_result.datasets or "edde" not in load_result.datasets:
        raise ValueError(
            "EDDE dataset not found in load_result. "
            "Please ensure the recipe includes the 'edde' dataset and run 'bd load' first."
        )

    # Get the first (and should be only) version of the edde dataset
    edde_versions = load_result.datasets["edde"]
    if not edde_versions:
        raise ValueError(
            "No EDDE dataset versions found in load_result. "
            "Please ensure the recipe includes the 'edde' dataset and run 'bd load' first."
        )

    # Get the first version's destination path
    edde_version_key = list(edde_versions.keys())[0]
    edde_data = edde_versions[edde_version_key]
    # The loaded EDDE dataset has a "dataset_files" subdirectory containing the actual CSVs
    old_edde_path = Path(edde_data.destination) / "dataset_files"

    # Get new build path from current build output directory (using build_name, not version)
    new_build_path = get_build_dir(PRODUCT_NAME, current_build_name) / "dataset_files"

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

    First checks BUILD_ENV_OUTPUT_DIR environment variable.
    If not set, falls back to DCPY_LIFECYCLE_DATA_DIR/build/edde/{version}
    where version is retrieved from recipe.lock.yml vars.VERSION

    Returns:
        Path to the build output directory

    Raises:
        BuildNotPlannedError: If recipe.lock.yml doesn't exist (fallback only)
        BuildDataNotLoadedError: If build_metadata.json doesn't exist (fallback only)
        ValueError: If VERSION not found in recipe.lock.yml vars (fallback only)
    """
    import os

    # Priority: BUILD_ENV_OUTPUT_DIR env var > calculated from recipe
    if "BUILD_ENV_OUTPUT_DIR" in os.environ:
        build_output_dir = Path(os.environ["BUILD_ENV_OUTPUT_DIR"])
        logger.info(
            f"Build output directory (from BUILD_ENV_OUTPUT_DIR): {build_output_dir}"
        )
        return build_output_dir

    # Fallback: calculate from recipe
    logger.warning(
        "BUILD_ENV_OUTPUT_DIR not set, falling back to recipe-based directory calculation. "
        "Consider setting BUILD_ENV_OUTPUT_DIR for consistent build output paths."
    )

    # Check that recipe has been planned
    recipe_lock_path = get_recipe_lock_path(PRODUCT_PATH)
    if not recipe_lock_path.exists():
        raise BuildNotPlannedError(
            f"Recipe has not been planned. Please run 'bd plan' first.\n"
            f"Expected recipe.lock.yml at: {recipe_lock_path}"
        )

    # Check that build metadata exists (data has been loaded)
    try:
        get_build_metadata(PRODUCT_PATH)
    except (FileNotFoundError, ValueError) as e:
        raise BuildDataNotLoadedError(
            f"Build data has not been loaded. Please run 'bd load' first.\nError: {e}"
        )

    # Get build_name from recipe lockfile
    recipe_lock = get_recipe_lock(PRODUCT_PATH)
    if not recipe_lock.build_name:
        raise ValueError(
            f"build_name not found in recipe lockfile at {get_recipe_lock_path(PRODUCT_PATH)}"
        )

    build_output_dir = get_build_dir(PRODUCT_NAME, recipe_lock.build_name)
    logger.info(f"Build output directory: {build_output_dir}")
    return build_output_dir


def clean_build_output_dir(categories: list[str] | None = None) -> None:
    """
    Clean category subfolders in the build output dataset_files directory before re-running a build.

    Args:
        categories: List of category names to clean (e.g., ["demographics", "economics"]).
                   If None, removes the dataset_files directory in the build output directory.

    Raises:
        BuildNotPlannedError: If recipe.lock.yml doesn't exist
        BuildDataNotLoadedError: If build_metadata.json doesn't exist
        ValueError: If VERSION not found in recipe.lock.yml vars
    """
    build_output_dir = get_build_output_dir()
    dataset_files_dir = build_output_dir / "dataset_files"

    if categories:
        logger.info(
            f"Cleaning categories {categories} in build output dataset_files directory: {dataset_files_dir}"
        )
    else:
        logger.info(
            f"Cleaning dataset_files directory in build output directory: {dataset_files_dir}"
        )

    if not dataset_files_dir.exists():
        logger.info(f"Dataset files directory does not exist yet: {dataset_files_dir}")
        return

    items_removed = 0

    if categories:
        # Only remove specific category directories within dataset_files/
        for category in categories:
            category_path = dataset_files_dir / category
            if category_path.exists() and category_path.is_dir():
                logger.info(f"Removing directory: {category_path}")
                shutil.rmtree(category_path)
                items_removed += 1
            elif category_path.exists():
                logger.info(f"Removing file: {category_path}")
                category_path.unlink()
                items_removed += 1
    else:
        # Remove entire dataset_files directory
        if dataset_files_dir.exists() and dataset_files_dir.is_dir():
            logger.info(f"Removing dataset_files directory: {dataset_files_dir}")
            shutil.rmtree(dataset_files_dir)
            items_removed += 1

    if items_removed == 0:
        logger.info("No items to remove")
    else:
        logger.info(f"Removed {items_removed} items")
