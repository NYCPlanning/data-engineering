"""Plan artifact management for builds lifecycle.

This module provides functions for uploading planned recipes to blob storage.
"""

from datetime import datetime
from pathlib import Path

import pytz

from dcpy.connectors.edm.models import PlanKey
from dcpy.lifecycle.builds.connector import get_plan_default_connector
from dcpy.utils import git
from dcpy.utils.logging import logger


def _generate_metadata() -> dict[str, str]:
    """Generates "standard" s3 metadata for our files"""
    metadata = {
        "date-created": datetime.now(pytz.timezone("America/New_York")).isoformat()
    }
    try:
        metadata["commit"] = git.commit_hash()
    except Exception:
        pass  # No git repo or commit available
    return metadata


def upload(
    recipe_lock_path: Path,
    product: str,
    version: str,
    acl: str = "private",
) -> PlanKey:
    """
    Uploads a planned recipe (recipe.lock.yml) using the configured plan connector.

    Args:
        recipe_lock_path: Path to recipe.lock.yml file or directory containing it
        product: Name of data product (e.g., "db-template")
        version: Version identifier for this plan (can be any string)
        acl: Access control level for uploaded files

    Returns:
        PlanKey: The key for the uploaded plan

    Raises:
        FileNotFoundError: If the recipe.lock.yml does not exist.
    """
    # Determine if recipe_lock_path is a file or directory
    if recipe_lock_path.is_file():
        lock_file = recipe_lock_path
        if recipe_lock_path.name != "recipe.lock.yml":
            raise ValueError(f"Expected recipe.lock.yml, got {recipe_lock_path.name}")
    elif recipe_lock_path.is_dir():
        lock_file = recipe_lock_path / "recipe.lock.yml"
        if not lock_file.exists():
            raise FileNotFoundError(f"recipe.lock.yml not found in {recipe_lock_path}")
    else:
        raise FileNotFoundError(f"Path {recipe_lock_path} does not exist")

    meta = _generate_metadata()

    logger.info(f"Uploading recipe.lock.yml to {product}/plan/{version}")

    get_plan_default_connector().push_versioned(
        key=product,
        version=version,
        source_path=str(lock_file),
        target_path="recipe.lock.yml",
        acl=acl,
        metadata=meta,
    )

    plan_key = PlanKey(product, version=version, revision="")

    logger.info(f"Successfully uploaded plan: {plan_key.path}")
    return plan_key


def download(
    product: str,
    version: str,
    destination_path: Path,
) -> Path:
    """
    Downloads a planned recipe (recipe.lock.yml) from blob storage.

    Args:
        product: Name of data product (e.g., "db-template")
        version: Version identifier for this plan (can be any string)
        destination_path: Local path to download to (can be file or directory)

    Returns:
        Path to the downloaded recipe.lock.yml file

    Raises:
        FileNotFoundError: If the plan doesn't exist in blob storage.
    """
    logger.info(f"Downloading plan from {product}/plan/{version}")

    result = get_plan_default_connector().pull_versioned(
        key=product,
        version=version,
        filepath="recipe.lock.yml",
        destination_path=destination_path,
    )

    downloaded_path = result.get("path")
    if not downloaded_path or not Path(downloaded_path).exists():
        raise FileNotFoundError(
            f"Failed to download recipe.lock.yml for {product}/plan/{version}"
        )

    logger.info(f"Successfully downloaded plan to: {downloaded_path}")
    return Path(downloaded_path)


def list_plans(product: str) -> list[str]:
    """List all plans for a product.

    Args:
        product: Product name

    Returns:
        List of version strings
    """
    return get_plan_default_connector().list_versions(product, sort_desc=True)


def get_recipe_lockfile(product: str, version: str):
    """Download and parse a recipe lockfile from blob storage.

    This is a convenience method that combines download() + recipe_from_yaml().

    Args:
        product: Name of data product (e.g., "db-template")
        version: Version identifier for this plan (can be any string)

    Returns:
        Recipe: Parsed recipe model

    Raises:
        FileNotFoundError: If the plan doesn't exist in blob storage.
    """
    from tempfile import TemporaryDirectory

    from dcpy.lifecycle.builds import plan as build_plan

    with TemporaryDirectory() as temp_dir:
        recipe_lock_path = download(
            product=product,
            version=version,
            destination_path=Path(temp_dir) / "recipe.lock.yml",
        )

        # Parse the recipe
        recipe = build_plan.recipe_from_yaml(recipe_lock_path)

        logger.info(f"Loaded recipe for {product} version {version}")
        return recipe
