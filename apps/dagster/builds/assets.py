import os
from pathlib import Path
from typing import Any

import yaml
from dagster import AssetExecutionContext, Config, MaterializeResult, asset
from pydantic import create_model

from dcpy import lifecycle
from dcpy.lifecycle.builds.plan import recipe_from_yaml

from .partitions import build_partition_def
from .resources import LocalStorageResource


def create_config_class(product_name: str, recipe_path: Path) -> type[Config]:
    """Dynamically create a Config class based on recipe template vars.

    Parses the recipe without rendering templates to extract the vars section keys.

    Args:
        product_name: Product name (e.g., 'edde')
        recipe_path: Path to the recipe.yml file

    Returns:
        A Pydantic Config class with fields for each template variable
    """
    # Parse recipe without rendering to get template variable names
    recipe = recipe_from_yaml(recipe_path, render_templates=False)
    template_vars = list(recipe.vars.keys()) if recipe.vars else []

    if not template_vars:
        # No template vars, create minimal config
        return type(f"PlanRecipeConfig_{product_name}", (Config,), {})

    # Create field definitions: {var_name: (str, ...)} where ... means required
    field_definitions: dict[str, Any] = {var: (str, ...) for var in template_vars}

    # Dynamically create Config class
    config_class = create_model(
        f"PlanRecipeConfig_{product_name}",
        __base__=Config,
        **field_definitions,
    )

    return config_class


def make_plan_recipe_asset(product: lifecycle.asset_models.Product):
    """Create a plan recipe asset for a specific product.

    Args:
        product: Product object with name and path attributes

    Returns:
        A Dagster asset function
    """
    config_class = create_config_class(product.name, product.recipe_path)

    @asset(
        name=f"plan_recipe_{product.name}",
        partitions_def=build_partition_def,
        group_name="build",
        tags={"product": product.name, "lifecycle_stage": "builds.plan"},
    )
    def _plan_recipe_asset(
        context: AssetExecutionContext,
        config: config_class,  # type: ignore
        local_storage: LocalStorageResource,
    ):
        """Plan recipe for product, resolving all template variables and versions."""
        from dcpy.lifecycle.builds.build import upload_build
        from dcpy.lifecycle.builds.plan import plan_recipe

        partition_key = context.partition_key

        # Set environment variables from config
        # Get all template vars from the config object
        template_vars = [field for field in config.__fields__.keys()]
        for var in template_vars:
            value = getattr(config, var)
            os.environ[var] = value
            context.log.info(f"Set {var}={value}")

        context.log.info(f"Planning recipe from {product.recipe_path}")

        # Plan the recipe (this will render templates and resolve versions)
        recipe = plan_recipe(product.recipe_path, version=partition_key)

        # Create temp build directory
        build_path = local_storage.get_path("builds", product.name, partition_key)

        # Write recipe.lock.yml to build directory
        lock_file = Path(build_path) / "recipe.lock.yml"
        with open(lock_file, "w", encoding="utf-8") as f:
            yaml.dump(recipe.model_dump(mode="json"), f)

        context.log.info(f"Wrote recipe.lock.yml to {lock_file}")

        # Upload build to S3 using existing functionality
        context.log.info(f"Uploading build from {build_path}")
        upload_build(build_path, recipe_lock_path=lock_file)

        return MaterializeResult(
            metadata={
                "recipe_path": str(product.recipe_path),
                "lock_file": str(lock_file),
                "build_path": str(build_path),
                "version": partition_key,
                "product": product.name,
                "template_vars": ", ".join(template_vars),
            }
        )

    return _plan_recipe_asset


# Generate assets for all products
products = lifecycle.list_products()
plan_recipe_assets = [make_plan_recipe_asset(product) for product in products]

# Export all assets
build_assets = plan_recipe_assets
