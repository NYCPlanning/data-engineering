"""Build lifecycle assets for all products."""

import os
from typing import Optional

import yaml
from builds.partitions import get_build_partition_def
from dagster_utils.partitions import parse_build_partition

from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    Config,
    MaterializeResult,
    asset,
)

# TODO: we need consistent import strategies. Unless these are referenced to build the dag, we should import them within the asset def.
from dcpy import lifecycle
from dcpy.lifecycle.builds import build as build_module
from dcpy.lifecycle.builds import load as build_load
from dcpy.lifecycle.builds import plan as build_plan
from dcpy.lifecycle.builds.artifacts import builds as builds_artifacts
from dcpy.lifecycle.builds.artifacts import plan as plan_artifacts
from dcpy.lifecycle.builds.artifacts import published
from dcpy.utils.logging import logger


# Config classes for each asset type
class PlanConfig(Config):
    """Configuration for plan asset."""

    build_note: Optional[str] = None  # Optional note for this build


class BuildConfig(Config):
    """Configuration for build asset."""

    build_note: Optional[str] = None  # Optional note (passed from plan if present)


class DraftConfig(Config):
    """Configuration for draft asset."""

    build_note: str  # REQUIRED: note for this draft revision


class ReviewConfig(Config):
    """Configuration for review asset."""

    draft_num: Optional[int] = None  # Optional: specify which draft to review


class InternalPublishConfig(Config):
    """Configuration for internal_publish asset."""

    draft_num: Optional[int] = None  # Optional: specify which draft to publish


class DistributeConfig(Config):
    """Configuration for distribute asset."""

    draft_num: Optional[int] = None  # Optional: specify which draft to distribute
    publish: bool = (
        False  # Whether to publish the distribution (e.g., Socrata revisions)
    )
    metadata_only: bool = False  # Only push metadata (including attachments)


def make_plan_asset(product: lifecycle.asset_models.Product):
    """Create a plan asset for a specific product.

    Args:
        product: Product object with name and path attributes

    Returns:
        A Dagster asset function
    """
    partition_def = get_build_partition_def(product.name)

    recipe_vars = build_plan.get_recipe_template_variables(product.recipe_path)
    config_annotations = {"build_note": Optional[str]}
    config_defaults = {"build_note": None}

    for var_name in recipe_vars:
        # Add each Jinja2 var as a REQUIRED string field (no Optional, no default)
        config_annotations[var_name] = str

    # Create the dynamic Config class
    DynamicPlanConfig = type(
        f"PlanConfig_{product.name}",
        (Config,),
        {
            "__annotations__": config_annotations,
            **config_defaults,
        },
    )

    @asset(
        name=f"plan_{product.name}",
        partitions_def=partition_def,
        group_name=f"build_{product.name}",
        tags={"product": product.name, "lifecycle_stage": "builds.plan"},
    )
    def _plan_asset(
        context: AssetExecutionContext,
        config: DynamicPlanConfig,
    ):
        """Plan recipe for product using lifecycle.builds.plan.plan()."""
        partition_key = context.partition_key
        parsed = parse_build_partition(partition_key)

        context.log.info(f"Planning {product.name}")
        context.log.info(f"  Version: {parsed.version}")
        context.log.info(f"  Branch: {parsed.branch}")
        context.log.info(f"  Timestamp: {parsed.timestamp}")
        if config.build_note:
            context.log.info(f"  Build note: {config.build_note}")

        # Set BUILD_ENV_* variables for recipe template rendering
        os.environ["BUILD_ENV_BRANCH"] = parsed.branch

        # Set all Jinja2 template vars from config (all are required)
        for var_name in recipe_vars:
            var_value = getattr(config, var_name)
            os.environ[var_name] = var_value
            context.log.info(f"  Set {var_name}: {var_value}")

        # Compute BUILD_ENGINE vars to be stored in recipe.env
        build_engine_schema = parsed.branch.lower().replace("-", "_")
        build_engine_db = f"db-{product.name}"
        context.log.info(f"  Computed BUILD_ENGINE_SCHEMA: {build_engine_schema}")
        context.log.info(f"  Computed BUILD_ENGINE_DB: {build_engine_db}")

        # Call lifecycle.builds.plan.plan()
        lock_file_path = build_plan.plan(
            recipe_file=product.recipe_path,
            version=parsed.version,
            build_name=partition_key,  # Pass partition key as build_name
            branch=parsed.branch,  # Pass branch from partition
            env_vars={
                "BUILD_ENGINE_SCHEMA": build_engine_schema,
                "BUILD_ENGINE_DB": build_engine_db,
            },
        )

        context.log.info(f"Recipe lock file created at: {lock_file_path}")

        # Upload recipe lock file to blob storage
        plan_key = plan_artifacts.upload(
            recipe_lock_path=lock_file_path,
            product=product.name,  # Connector handles db- prefix mapping
            version=partition_key,  # Use the full partition key as the version
        )

        context.log.info(f"Plan uploaded: {plan_key}")

        # Build metadata dictionary with standard fields
        metadata = {
            "product": product.name,
            "version": parsed.version,
            "branch": parsed.branch,
            "timestamp": parsed.timestamp,
            "build_note": config.build_note or "",
            "lock_file_path": str(lock_file_path),
            "plan_key": str(plan_key.path),
        }

        # Add all recipe vars to metadata so build can access them
        for var_name in recipe_vars:
            var_value = getattr(config, var_name)
            metadata[var_name] = var_value

        return MaterializeResult(metadata=metadata)

    return _plan_asset


def make_build_asset(product: lifecycle.asset_models.Product):
    """Create a build asset for a specific product.

    Args:
        product: Product object with name and path attributes

    Returns:
        A Dagster asset function
    """
    # Get product-specific partition definition
    partition_def = get_build_partition_def(product.name)

    @asset(
        name=f"build_{product.name}",
        partitions_def=partition_def,
        group_name=f"build_{product.name}",
        tags={"product": product.name, "lifecycle_stage": "builds.build"},
        automation_condition=AutomationCondition.eager(),
        deps=[f"plan_{product.name}"],
    )
    def _build_asset(
        context: AssetExecutionContext,
        config: BuildConfig,
    ):
        """Build product using lifecycle.builds.build.build() + upload()."""
        import os

        from dcpy.lifecycle.config import get_build_dir

        partition_key = context.partition_key
        parsed = parse_build_partition(partition_key)

        context.log.info(f"Building {product.name}")
        context.log.info(f"  Version: {parsed.version}")
        context.log.info(f"  Branch: {parsed.branch}")
        context.log.info(f"  Timestamp: {parsed.timestamp}")

        # Download recipe to build directory first
        # We use the recipe for both build_name (for directory) and dataset version
        # Note: We create build_output_dir with a temp name first, then get actual build_name from recipe
        temp_build_dir = get_build_dir(product.name, partition_key)
        temp_build_dir.mkdir(parents=True, exist_ok=True)

        # Download recipe to build directory
        recipe_lock_path = plan_artifacts.download(
            product=product.name,
            version=partition_key,
            destination_path=temp_build_dir,
        )
        context.log.info(f"Downloaded recipe to build directory: {recipe_lock_path}")

        # Parse recipe to get build_name and dataset version
        recipe = build_plan.recipe_from_yaml(recipe_lock_path)
        recipe_version = recipe.version
        context.log.info(f"  Dataset version from recipe: {recipe_version}")
        context.log.info(f"  Build name: {recipe.build_name}")

        # If recipe.build_name differs from partition_key, we'd need to rename the directory
        # but typically they should match, so we just use the temp_build_dir as is
        build_output_dir = temp_build_dir

        # Set environment variables from recipe.env (needed by load and build)
        if recipe.env:
            for key, value in recipe.env.items():
                os.environ[key] = value
                context.log.info(f"Set recipe env: {key}={value}")

        # Set additional environment variables for build
        os.environ["BUILD_ENV_OUTPUT_DIR"] = str(build_output_dir)
        context.log.info(f"Set BUILD_ENV_OUTPUT_DIR: {build_output_dir}")

        # Configure file logging now that BUILD_ENV_OUTPUT_DIR is set
        logger.set_file_output(build_output_dir)

        # Load source data from recipe into build database
        load_result = build_load.load_source_data_from_resolved_recipe(
            recipe_or_path=recipe_lock_path,  # Pass path to recipe in build directory
            clear_pg_schema=True,  # Clear schema before loading
            # target_schema will be read from BUILD_ENGINE_SCHEMA env var (set in recipe env)
        )
        context.log.info(f"Loaded {len(load_result.datasets)} datasets")

        # Execute build commands from recipe
        # BUILD_ENV_OUTPUT_DIR already set in environment above
        output_path = build_module.build(
            product_path=product.path,
            build_directory=build_output_dir,  # Recipe in build directory
        )

        context.log.info(f"Build completed. Output: {output_path}")

        # Upload build to storage
        build_key = builds_artifacts.upload(
            output_path=output_path,
            product=product.name,  # Connector handles db- prefix mapping
            build=partition_key,  # Use full partition key as build identifier
            acl="public-read",  # Make builds publicly accessible
        )

        context.log.info(f"Build uploaded: {build_key}")

        return MaterializeResult(
            metadata={
                "product": product.name,
                "version": recipe_version,
                "branch": parsed.branch,
                "timestamp": parsed.timestamp,
                "build_note": config.build_note or "",
                "build_key": str(build_key),
                "output_path": str(output_path),
            }
        )

    return _build_asset


def make_draft_asset(product: lifecycle.asset_models.Product):
    """Create a draft asset for a specific product.

    Args:
        product: Product object with name and path attributes

    Returns:
        A Dagster asset function
    """
    # Get product-specific partition definition
    partition_def = get_build_partition_def(product.name)

    @asset(
        name=f"draft_{product.name}",
        partitions_def=partition_def,
        group_name=f"build_{product.name}",
        tags={"product": product.name, "lifecycle_stage": "builds.draft"},
        # No automation - draft creation is a manual decision after build
        deps=[f"build_{product.name}"],
    )
    def _draft_asset(
        context: AssetExecutionContext,
        config: DraftConfig,
    ):
        """Promote build to draft using lifecycle.builds.artifacts.builds.promote_to_draft()."""
        from dcpy.lifecycle.builds.artifacts import plan as plan_artifacts

        partition_key = context.partition_key
        parsed = parse_build_partition(partition_key)

        context.log.info(f"Creating draft for {product.name}")
        context.log.info(f"  Version: {parsed.version}")
        context.log.info(f"  Branch: {parsed.branch}")
        context.log.info(f"  Timestamp: {parsed.timestamp}")
        context.log.info(f"  Build note: {config.build_note}")

        # Get dataset version from recipe lockfile
        recipe = plan_artifacts.get_recipe_lockfile(
            product=product.name,
            version=partition_key,
        )
        recipe_version = recipe.version
        context.log.info(f"  Dataset version from recipe: {recipe_version}")

        # Promote build to draft
        draft_key = builds_artifacts.promote_to_draft(
            product=product.name,
            build=partition_key,  # Use full partition key to match build upload
            draft_revision_summary=config.build_note,
            version=recipe_version,  # Pass version from recipe
        )

        context.log.info(f"Draft created: {draft_key}")

        return MaterializeResult(
            metadata={
                "product": product.name,
                "version": recipe_version,  # Dataset version from recipe
                "branch": parsed.branch,
                "timestamp": parsed.timestamp,
                "build_note": config.build_note,
                "draft_key": str(draft_key),
                "draft_version": draft_key.version,
                "draft_revision": draft_key.revision,
            }
        )

    return _draft_asset


def make_review_asset(product: lifecycle.asset_models.Product):
    """Create a review asset for a specific product.

    Args:
        product: Product object with name and path attributes

    Returns:
        A Dagster asset function
    """
    # Get product-specific partition definition
    partition_def = get_build_partition_def(product.name)

    @asset(
        name=f"review_{product.name}",
        partitions_def=partition_def,
        group_name=f"build_{product.name}",
        tags={"product": product.name, "lifecycle_stage": "builds.review"},
        # No automation - review is a manual approval gate
        deps=[f"draft_{product.name}"],
    )
    def _review_asset(
        context: AssetExecutionContext,
        config: ReviewConfig,
    ):
        """Manual review approval (logging only - no dcpy function)."""
        from dagster import AssetKey
        from dagster._core.storage.event_log.base import AssetRecordsFilter

        partition_key = context.partition_key
        parsed = parse_build_partition(partition_key)

        # Get version and draft revision from upstream draft asset for this specific partition
        records = context.instance.fetch_materializations(
            records_filter=AssetRecordsFilter(
                asset_key=AssetKey([f"draft_{product.name}"]),
                asset_partitions=[partition_key],
            ),
            limit=1,
        )

        version = None
        draft_revision = None
        if records.records:
            event_record = records.records[0]
            if event_record.asset_materialization:
                metadata = event_record.asset_materialization.metadata
                version = (
                    metadata.get("version").value if metadata.get("version") else None
                )
                draft_revision = (
                    metadata.get("draft_revision").value
                    if metadata.get("draft_revision")
                    else None
                )

        context.log.info(f"Review approval for {product.name}")
        context.log.info(f"  Version: {version}")
        context.log.info(f"  Branch: {parsed.branch}")
        context.log.info(f"  Timestamp: {parsed.timestamp}")
        context.log.info(f"  Draft revision: {draft_revision}")

        context.log.info("Review approved - ready for internal publish")

        # Mock GitHub issue URL (TODO: integrate with actual review system)
        mock_github_issue = (
            f"https://github.com/mock-org/mock-repo/issues/{version}-review"
        )

        return MaterializeResult(
            metadata={
                "product": product.name,
                "version": version,  # Dataset version from draft (which got it from recipe)
                "branch": parsed.branch,
                "timestamp": parsed.timestamp,
                "draft_revision": draft_revision,  # e.g., "2-ar_fix_the_thing"
                "github_issue": mock_github_issue,
            }
        )

    return _review_asset


def make_internal_publish_asset(product: lifecycle.asset_models.Product):
    """Create an internal_publish asset for a specific product.

    Args:
        product: Product object with name and path attributes

    Returns:
        A Dagster asset function
    """
    # Get product-specific partition definition
    partition_def = get_build_partition_def(product.name)

    @asset(
        name=f"internal_publish_{product.name}",
        partitions_def=partition_def,
        group_name=f"build_{product.name}",
        tags={"product": product.name, "lifecycle_stage": "builds.internal_publish"},
        automation_condition=AutomationCondition.eager(),
        deps=[f"review_{product.name}"],
    )
    def _internal_publish_asset(
        context: AssetExecutionContext,
        config: InternalPublishConfig,
    ):
        """Internal publish using lifecycle.builds.artifacts.published.publish()."""
        from dagster import AssetKey
        from dagster._core.storage.event_log.base import AssetRecordsFilter
        from dcpy.lifecycle.builds.artifacts import plan as plan_artifacts

        partition_key = context.partition_key
        parsed = parse_build_partition(partition_key)

        # Get dataset version from recipe lockfile
        recipe = plan_artifacts.get_recipe_lockfile(
            product=product.name,
            version=partition_key,
        )
        version_str = recipe.version
        context.log.info(f"Dataset version from recipe: {version_str}")

        # Get draft revision from upstream review asset
        records = context.instance.fetch_materializations(
            records_filter=AssetRecordsFilter(
                asset_key=AssetKey([f"review_{product.name}"]),
                asset_partitions=[partition_key],
            ),
            limit=1,
        )

        if not records.records:
            raise ValueError(
                f"No review metadata found for {product.name} partition {partition_key}"
            )

        event_record = records.records[0]
        if not event_record.asset_materialization:
            raise ValueError(
                f"No asset materialization in review record for {product.name} partition {partition_key}"
            )

        metadata = event_record.asset_materialization.metadata
        draft_revision = (
            metadata.get("draft_revision").value
            if metadata.get("draft_revision")
            else None
        )

        if not draft_revision:
            raise ValueError(
                f"No draft_revision found in review metadata for {product.name}"
            )

        # Construct full draft version: version from recipe + revision from draft
        full_draft_version = f"{version_str}.{draft_revision}"
        revision_num = int(draft_revision.split("-")[0])

        context.log.info(f"Internal publish for {product.name}")
        context.log.info(f"  Draft version: {full_draft_version}")
        context.log.info(f"  Publishing as version: {version_str}")
        context.log.info(f"  Revision: {revision_num}")

        # Publish draft to internal storage
        publish_key = published.publish(
            product=product.name,
            version=version_str,
            draft_revision_num=revision_num,
            latest=True,  # Also update 'latest' folder
            skip_version_validation=True,  # Skip validation (published folder may have old versions)
        )

        context.log.info(f"Published: {publish_key}")

        return MaterializeResult(
            metadata={
                "product": product.name,
                "version": version_str,  # Use actual version from build_metadata.json
                "branch": parsed.branch,
                "timestamp": parsed.timestamp,
                "draft_revision": draft_revision,
                "publish_key": str(publish_key),
            }
        )

    return _internal_publish_asset


def _destination_key_to_asset_name(destination_key: str) -> str:
    """Convert a destination key to an asset-safe name.

    Args:
        destination_key: Destination key in format product.dataset.destination

    Returns:
        Asset-safe name with double underscores like product__dataset__destination
    """
    from dcpy.product_metadata.keys import DestinationKey

    try:
        dest_key = DestinationKey.from_path_str(destination_key)
        # Use double underscores to separate components
        return f"{dest_key.product}__{dest_key.dataset}__{dest_key.destination_id}"
    except (ValueError, IndexError):
        # Fallback if not in expected format
        return destination_key.replace(".", "__")


def make_distribute_asset(product: lifecycle.asset_models.Product, destination: str):
    """Create a distribute asset for a specific product and destination.

    Args:
        product: Product object with name and path attributes
        destination: Full destination key in format {product}.{dataset}.{destination}

    Returns:
        A Dagster asset function
    """
    # Get product-specific partition definition
    partition_def = get_build_partition_def(product.name)

    # Convert destination key to asset-safe name
    sanitized_dest = _destination_key_to_asset_name(destination)

    @asset(
        name=f"distribute_{product.name}_{sanitized_dest}",
        partitions_def=partition_def,
        group_name=f"build_{product.name}",
        tags={
            "product": product.name,
            "destination": destination,
            "destination_key": destination,
            "lifecycle_stage": "builds.distribute",
        },
        automation_condition=AutomationCondition.eager(),
        deps=[f"internal_publish_{product.name}"],
    )
    def _distribute_asset(
        context: AssetExecutionContext,
        config: DistributeConfig,
    ):
        """Distribute build to destination using lifecycle.distribute."""
        from dagster import AssetKey
        from dagster._core.storage.event_log.base import AssetRecordsFilter
        from dcpy.lifecycle import distribute

        partition_key = context.partition_key
        parsed = parse_build_partition(partition_key)

        # Get published version from upstream internal_publish asset

        records = context.instance.fetch_materializations(
            records_filter=AssetRecordsFilter(
                asset_key=AssetKey([f"internal_publish_{product.name}"]),
                asset_partitions=[partition_key],
            ),
            limit=1,
        )

        if not records.records:
            raise ValueError(
                f"No publish metadata found for {product.name} partition {partition_key}"
            )

        event_record = records.records[0]
        if not event_record.asset_materialization:
            raise ValueError(
                f"No asset materialization in publish record for {product.name} partition {partition_key}"
            )

        metadata = event_record.asset_materialization.metadata
        published_version = (
            metadata.get("version").value if metadata.get("version") else parsed.version
        )

        context.log.info(f"Distributing {product.name} to {destination}")
        context.log.info(f"  Published version: {published_version}")
        context.log.info(f"  Branch: {parsed.branch}")
        context.log.info(f"  Timestamp: {parsed.timestamp}")

        # Download from published location
        published_path = published.download(
            product=product.name,  # Connector handles db- prefix mapping
            version=published_version,
        )

        context.log.info(f"Downloaded published build from: {published_path}")

        # Distribute to the specified destination using the full destination key
        result = distribute.distribute_build(
            build_path=published_path,
            destination_key=destination,
            version=published_version,
            publish=config.publish,
            metadata_only=config.metadata_only,
        )

        # Check if distribution succeeded
        if not result.success:
            error_msg = result.result_details or "No error details"
            raise Exception(f"Distribution to {destination} failed: {error_msg}")

        context.log.info(f"Distribution succeeded: {result.result_summary}")

        return MaterializeResult(
            metadata={
                "product": product.name,
                "destination": destination,
                "version": published_version,
                "branch": parsed.branch,
                "timestamp": parsed.timestamp,
                "success": result.success,
                "result_summary": result.result_summary or "",
                "result_details": result.result_details or "",
            }
        )

    return _distribute_asset


# TODO: why not just use the recipe model for this?
def get_destinations_from_recipe(product: lifecycle.asset_models.Product) -> list[str]:
    """Read destinations from product's recipe.yml.

    Args:
        product: Product object with recipe_path attribute

    Returns:
        List of destination strings, or ["mock_destination"] if not specified
    """
    try:
        with open(product.recipe_path, "r") as f:
            recipe_data = yaml.safe_load(f)

        # Check if distribution.destination_ids exists
        if (
            recipe_data
            and "distribution" in recipe_data
            and "destination_ids" in recipe_data["distribution"]
        ):
            destination_ids = recipe_data["distribution"]["destination_ids"]
            if isinstance(destination_ids, list) and len(destination_ids) > 0:
                return destination_ids

    except Exception:
        # If any error reading recipe, fall back to mock
        pass

    # Return empty list if no destinations (don't create mock assets)
    return []


# TODO: is this a standard way to structure these modules? Having this wrapped up in a function seems cleaner, but if this is standard, I'll defer.
# Generate assets for all products
products = lifecycle.list_products()

plan_assets = [make_plan_asset(product) for product in products]
build_assets_list = [make_build_asset(product) for product in products]
draft_assets = [make_draft_asset(product) for product in products]
review_assets = [make_review_asset(product) for product in products]
internal_publish_assets = [make_internal_publish_asset(product) for product in products]

# Create distribute assets based on destinations in each product's recipe
distribute_assets_list = []
for product in products:
    destinations = get_destinations_from_recipe(product)
    for destination in destinations:
        distribute_assets_list.append(make_distribute_asset(product, destination))

# Export all assets
build_assets = (
    plan_assets
    + build_assets_list
    + draft_assets
    + review_assets
    + internal_publish_assets
    + distribute_assets_list
)
