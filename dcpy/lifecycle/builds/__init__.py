from pathlib import Path

from dcpy.lifecycle.builds.connector import get_recipes_default_connector
from dcpy.lifecycle.builds.models import Recipe
from dcpy.lifecycle.builds.plan import recipe_from_yaml

# TODO: Move these out of here
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

BUILD_PLAN_ARTIFACTS = [
    "recipe.lock.yml",
    "build_metadata.json",
    "source_data_versions.csv",
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


def get_recipe(
    product_path: Path,
    recipe_name: str | None = None,
    render_templates: bool = True,
    vars: dict[str, str] | None = None,
) -> Recipe:
    """Load a recipe file as a pydantic Recipe model.

    Args:
        product_path: Path to the product directory
        recipe_name: Name of the recipe file (e.g., "my-recipe").
                    If None, defaults to "recipe"
        render_templates: If True, render Jinja2 templates from vars or BUILD_ENV_* vars.
                         If False, preserve templates as strings (for DAG generation).
        vars: Optional dict of template variables. If provided, these are used instead
              of BUILD_ENV_* environment variables.

    Returns:
        Recipe pydantic model
    """
    recipe_path = get_recipe_path(product_path, recipe_name)
    return recipe_from_yaml(recipe_path, render_templates=render_templates, vars=vars)


def get_recipe_lock(
    product_path: Path,
    recipe_name: str | None = None,
    render_templates: bool = True,
    vars: dict[str, str] | None = None,
) -> Recipe:
    """Load a recipe lockfile as a pydantic Recipe model.

    Args:
        product_path: Path to the product directory
        recipe_name: Name of the recipe file (e.g., "my-recipe").
                    If None, defaults to "recipe"
        render_templates: If True, render Jinja2 templates from vars or BUILD_ENV_* vars.
                         If False, preserve templates as strings (for DAG generation).
        vars: Optional dict of template variables. If provided, these are used instead
              of BUILD_ENV_* environment variables.

    Returns:
        Recipe pydantic model from the lockfile
    """
    recipe_lock_path = get_recipe_lock_path(product_path, recipe_name)
    return recipe_from_yaml(
        recipe_lock_path, render_templates=render_templates, vars=vars
    )


__all__ = [
    "get_recipes_default_connector",
    "get_recipe_path",
    "get_recipe_lock_path",
    "get_build_metadata_path",
    "get_recipe",
    "get_recipe_lock",
    "get_recipes_default_connector",
]
