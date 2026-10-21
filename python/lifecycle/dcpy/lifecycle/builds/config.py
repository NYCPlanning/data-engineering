import os
from pathlib import Path
from typing import TYPE_CHECKING

from dcpy.lifecycle import config

if TYPE_CHECKING:
    from dcpy.lifecycle.builds.models import Recipe

# Build configuration constants
BUILD_ARTIFACT_DIRS = ["target", "attachments", "dataset_files"]
BUILD_STAGE_KEY = "builds.build"  # Recipe stage_config key for build commands
# Recipe stage_config key for packaging commands, run by `builds export package` after
# its built-in packaging (e.g. generating docs into attachments/)
PACKAGE_STAGE_KEY = "builds.package"
# Recipe stage_config key for QA commands, run by `builds qa run` (they see BUILD_DIR,
# PACKAGE_DIR and QA_DIR)
QA_STAGE_KEY = "builds.qa"


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


def uses_stage_dirs(recipe: "Recipe") -> bool:
    """Whether a recipe opts into separate build/, package/ and qa/ stage dirs, by using
    a feature that needs them: parquet-first packaging, export groups, or packaging / QA
    stage commands. Other recipes keep a single build dir holding every output."""
    exports = recipe.exports
    return bool(
        (
            exports
            and (
                exports.package_from_parquet
                or any(dataset.group for dataset in exports.datasets)
            )
        )
        or PACKAGE_STAGE_KEY in recipe.stage_config
        or QA_STAGE_KEY in recipe.stage_config
    )


def get_build_root(product: str, version: str) -> Path:
    """One directory per product version holding every stage's outputs, so a build can
    be cleared with one delete: {DCPY_LIFECYCLE_DATA_DIR}/builds/build/{product}/{version}.
    With stage dirs (see uses_stage_dirs), build/, package/ and qa/ sit inside it.
    BUILD_ENV_ROOT_DIR overrides it."""
    if "BUILD_ENV_ROOT_DIR" in os.environ:
        return Path(os.environ["BUILD_ENV_ROOT_DIR"])
    return config.get_build_dir(product, version)


def get_build_output_dir(product: str, version: str, staged: bool = False) -> Path:
    """The build stage's directory (duckdb file, recipe lock, exports): the build root,
    or <root>/build for a recipe with stage dirs (`staged`, see uses_stage_dirs).

    BUILD_ENV_OUTPUT_DIR overrides it. Load, build, and export run as separate
    processes, so they all resolve it here to agree.
    """
    if "BUILD_ENV_OUTPUT_DIR" in os.environ:
        return Path(os.environ["BUILD_ENV_OUTPUT_DIR"])
    root = get_build_root(product, version)
    return root / "build" if staged else root


def get_recipe_build_dir(product: str, version: str, recipe: "Recipe") -> Path:
    """The build stage's directory for `recipe`: <root>/build if it uses stage dirs, else
    the build root. For callers that name the product and version themselves."""
    return get_build_output_dir(product, version, uses_stage_dirs(recipe))


def get_duckdb_path(
    product: str, version: str, filename: str | None = None, staged: bool = False
) -> Path:
    """``filename`` (a recipe's ``duckdb_filename``) defaults to ``{product}_{version}.duckdb``."""
    return get_build_output_dir(product, version, staged) / (
        filename or f"{product}_{version}.duckdb"
    )


def get_package_output_dir(product: str, version: str) -> Path:
    """The package stage's directory (dataset_files, attachments, zip): <root>/package."""
    return get_build_root(product, version) / "package"


def get_qa_output_dir(product: str, version: str) -> Path:
    """The QA stage's directory: <root>/qa."""
    return get_build_root(product, version) / "qa"


def stage_dirs_env(product: str, version: str) -> dict[str, str]:
    """BUILD_DIR / PACKAGE_DIR / QA_DIR, for a stage-dirs recipe's stage commands."""
    return {
        "BUILD_DIR": str(
            get_build_output_dir(product, version, staged=True).absolute()
        ),
        "PACKAGE_DIR": str(get_package_output_dir(product, version).absolute()),
        "QA_DIR": str(get_qa_output_dir(product, version).absolute()),
    }
