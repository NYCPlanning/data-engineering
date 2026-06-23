"""Asset for creating build partitions and triggering materializations."""

from builds.partitions import get_build_partition_def
from dagster_utils.partitions import create_build_partition

from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    asset,
)
from dcpy import lifecycle


class CreateBuildPartitionConfig(Config):
    """Configuration for creating a build partition."""

    product: str  # Product name (e.g., "edde", "pluto")
    version: str  # Version string (e.g., "26v1.1", "26v2")
    branch: str  # Branch name (e.g., "main", "fix-bug")
    # TODO: Add version validation


@asset(
    name="create_build_partition",
    group_name="builds",
    tags={"lifecycle_stage": "builds.partition_creator"},
)
def create_build_partition_asset(
    context: AssetExecutionContext,
    config: CreateBuildPartitionConfig,
):
    """Create a build partition and trigger materialization of plan asset.

    This asset validates the product name, creates a partition in the format
    {version}:{timestamp}:{branch}, adds it to the partition definition,
    and triggers materialization of the plan asset for that partition.

    Args:
        config: Configuration containing product, version, and branch

    Returns:
        MaterializeResult with partition key and next steps

    Raises:
        ValueError: If product name is invalid

    TODO: Add version validation
    """
    # Validate product name
    products = lifecycle.list_products()
    product_names = [p.name for p in products]

    if config.product not in product_names:
        raise ValueError(
            f"Invalid product name: '{config.product}'. "
            f"Valid products: {', '.join(product_names)}"
        )

    # Create partition key with current timestamp
    partition_key = create_build_partition(
        version=config.version,
        branch=config.branch,
    )

    context.log.info(f"Creating build partition: {partition_key}")

    # Get product-specific partition definition
    partition_def = get_build_partition_def(config.product)

    # Add partition to the product-specific partition definition
    instance = context.instance
    instance.add_dynamic_partitions(
        partitions_def_name=partition_def.name,
        partition_keys=[partition_key],
    )

    context.log.info(f"✓ Partition created: {partition_key}")
    context.log.info(f"✓ Product: {config.product}")
    context.log.info(f"✓ Version: {config.version}")
    context.log.info(f"✓ Branch: {config.branch}")
    context.log.info("")
    context.log.info("Next steps:")
    context.log.info(
        f"  1. Materialize: plan_{config.product} (partition: {partition_key})"
    )
    context.log.info(
        f"  2. build_{config.product} will auto-materialize after plan completes"
    )
    context.log.info(
        f"  3. Manually materialize draft_{config.product} when ready to hand off"
    )

    # TODO: Automatically trigger materialization of plan asset
    # This requires using run_request or jobs, which we'll add later

    return MaterializeResult(
        metadata={
            "partition_key": partition_key,
            "product": config.product,
            "version": config.version,
            "branch": config.branch,
            "plan_asset": f"plan_{config.product}",
            "next_step": f"Materialize plan_{config.product} with partition {partition_key}",
        }
    )
