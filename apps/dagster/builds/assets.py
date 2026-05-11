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
    Always includes an optional plan_note field for revision tracking.

    Args:
        product_name: Product name (e.g., 'edde')
        recipe_path: Path to the recipe.yml file

    Returns:
        A Pydantic Config class with fields for each template variable plus plan_note
    """
    from pydantic import Field, field_validator

    # Parse recipe without rendering to get template variable names
    recipe = recipe_from_yaml(recipe_path, render_templates=False)
    template_vars = [k for k, v in (recipe.vars or {}).items() if v.startswith("{{")]

    # Create field definitions: {var_name: (str, ...)} where ... means required
    field_definitions: dict[str, Any] = {var: (str, ...) for var in template_vars}

    # Add optional plan_note field (alphanumeric + underscores only, or empty)
    field_definitions["plan_note"] = (
        str,
        Field(
            default="",
            description="Optional note for plan revision (alphanumeric and underscores only)",
        ),
    )

    # Create validator for plan_note
    def validate_plan_note(cls, v: str) -> str:
        if v and not all(c.isalnum() or c == "_" for c in v):
            raise ValueError(
                "plan_note must only contain alphanumeric characters and underscores"
            )
        return v

    # Dynamically create Config class
    config_class = create_model(
        f"PlanRecipeConfig_{product_name}",
        __base__=Config,
        __validators__={"plan_note": field_validator("plan_note")(validate_plan_note)},
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
        from dcpy.lifecycle.builds.connector import get_plan_default_connector
        from dcpy.lifecycle.builds.plan import plan_recipe

        partition_key = context.partition_key

        # Extract plan_note separately from template vars
        plan_note = config.plan_note

        # Build vars dict from config (excluding plan_note) to pass to plan_recipe
        template_vars = [
            field for field in config.__fields__.keys() if field != "plan_note"
        ]
        vars_dict = (
            {var: getattr(config, var) for var in template_vars}
            if template_vars
            else None
        )

        if vars_dict:
            context.log.info(f"Using template vars: {list(vars_dict.keys())}")
        if plan_note:
            context.log.info(f"Plan note: {plan_note}")

        context.log.info(f"Planning recipe from {product.recipe_path}")

        # Plan the recipe (this will render templates and resolve versions)
        recipe = plan_recipe(product.recipe_path, version=partition_key, vars=vars_dict)

        # Create temp build directory
        build_path = local_storage.get_path("builds", product.name, partition_key)

        # Write recipe.lock.yml to build directory
        lock_file = Path(build_path) / "recipe.lock.yml"
        with open(lock_file, "w", encoding="utf-8") as f:
            yaml.dump(recipe.model_dump(mode="json"), f)

        context.log.info(f"Wrote recipe.lock.yml to {lock_file}")

        # Upload plan to S3 using default plan connector (revision auto-generated)
        context.log.info(f"Uploading plan from {build_path}")
        plan_connector = get_plan_default_connector()
        result = plan_connector.push_versioned(
            key=recipe.product,
            version=partition_key,
            source_path=str(build_path),
            plan_note=plan_note,
        )

        return MaterializeResult(
            metadata={
                "recipe_path": str(product.recipe_path),
                "lock_file": str(lock_file),
                "build_path": str(build_path),
                "upload_result": str(result),
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
