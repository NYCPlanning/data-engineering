from pathlib import Path

# Build configuration constants
BUILD_REPO = "data-engineering"

BUILD_ARTIFACT_DIRS = ["target"]

BUILD_DBS = [
    "db-cbbr",
    "db-cdbg",
    "db-ceqr",
    "db-checkbook",
    "db-colp",
    "db-cpdb",
    "db-devdb",
    "db-facilities",
    "db-green-fast-track",
    # "db-cscl", we need to preserve schemas while this data product is in development
    "db-pluto",
    "db-template",
    "db-ztl",
    "kpdb",
]


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
    """Get the path to a recipe lockfile for a product.

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
