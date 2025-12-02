from pathlib import Path
from typing import Any, Dict, List, Optional
import re
import subprocess
import yaml

from dagster import (
    asset,
    AssetIn,
    DynamicPartitionsDefinition,
    MaterializeResult,
    AssetExecutionContext,
)
from partitions import ingest_partition_def
from resources import LocalStorageResource

PROJECT_ROOT = Path(__file__).parent.parent.parent
PRODUCTS_PATH = PROJECT_ROOT / "products" / "products.yml"
INGEST_TEMPLATES_PATH = PROJECT_ROOT / "ingest_templates"

# Review assets now share the same partition as build assets for simpler workflow


def load_products() -> List[Dict[str, Any]]:
    with open(PRODUCTS_PATH) as f:
        data = yaml.safe_load(f)
    return data["products"]


def get_ingest_template_ids() -> List[str]:
    templates = []
    for file in INGEST_TEMPLATES_PATH.glob("*.yml"):
        templates.append(file.stem)
    return templates


def build_ingest_to_products_mapping() -> Dict[str, List[str]]:
    """
    Scan all product recipes to find which products use which ingest datasets.
    Returns a mapping from ingest dataset name to list of products that use it.
    """
    ingest_to_products = {}
    products_dir = PROJECT_ROOT / "products"

    # Scan all product directories for recipe.yml files
    for product_dir in products_dir.iterdir():
        if not product_dir.is_dir():
            continue

        recipe_file = product_dir / "recipe.yml"
        if not recipe_file.exists():
            continue

        try:
            with open(recipe_file) as f:
                recipe_data = yaml.safe_load(f)

            # Get product name from recipe or directory name
            product_name = recipe_data.get("product", product_dir.name)

            # Look for datasets in inputs.datasets
            inputs = recipe_data.get("inputs", {})
            datasets = inputs.get("datasets", [])

            for dataset in datasets:
                if isinstance(dataset, dict) and "name" in dataset:
                    dataset_name = dataset["name"]
                elif isinstance(dataset, str):
                    dataset_name = dataset
                else:
                    continue

                # Add this product to the list for this dataset
                if dataset_name not in ingest_to_products:
                    ingest_to_products[dataset_name] = []

                if product_name not in ingest_to_products[dataset_name]:
                    ingest_to_products[dataset_name].append(product_name)

        except Exception as e:
            # Skip recipes that can't be parsed
            print(f"Warning: Could not parse recipe for {product_dir.name}: {e}")
            continue

    return ingest_to_products


def make_ingest_asset(template_id: str, using_products: Optional[List[str]] = None):
    # Build tags - include products that use this ingest dataset
    tags = {"template": template_id, "lifecycle_stage": "ingest"}

    if using_products:
        # Add individual product tags (clean product names for valid tag keys)
        for product in using_products:
            clean_product = product.replace("-", "_").replace(" ", "_")
            tags[f"used_by_{clean_product}"] = "true"

        # Add a combined tag for easy filtering (use underscores instead of commas)
        clean_products = [
            p.replace("-", "_").replace(" ", "_") for p in sorted(using_products)
        ]
        tags["used_by_products"] = "_".join(clean_products)
        tags["product_count"] = str(len(using_products))

    @asset(
        name=f"ingest_{template_id}",
        partitions_def=ingest_partition_def,
        group_name="ingest",
        tags=tags,
    )
    def _ingest_asset(
        context: AssetExecutionContext, local_storage: LocalStorageResource
    ):
        from dcpy.lifecycle.ingest.run import ingest

        partition_key = context.partition_key
        output_path = local_storage.get_path("ingest", template_id, partition_key)
        ingest(
            dataset_id=template_id,
            version=partition_key,
            push=False,
            local_file_path=output_path,
        )
        return MaterializeResult(
            metadata={"output_path": str(output_path), "version": partition_key}
        )

    return _ingest_asset


def make_build_asset_group(product: Dict[str, Any]):
    product_id = product["id"]
    build_command = product["build_command"]
    asset_name_base = product_id.replace(" ", "_").replace("-", "_")

    build_partition_def = DynamicPartitionsDefinition(name=f"build_{asset_name_base}")

    # Create enhanced tags with product metadata (clean values for Dagster tag restrictions)
    base_tags = {
        "product": asset_name_base,
        "product_id": asset_name_base,  # Use cleaned name instead of original
        "lifecycle_stage": "build",
        "has_build_command": "true" if build_command else "false",
    }

    # Add optional product metadata (clean for Dagster tag restrictions)
    if "description" in product:
        clean_desc = (
            product["description"].replace(" ", "_").replace("-", "_")[:63]
        )  # Clean and truncate
        base_tags["product_description"] = clean_desc
    if "version_strategy" in product:
        clean_strategy = product["version_strategy"].replace(" ", "_").replace("-", "_")
        base_tags["version_strategy"] = clean_strategy
    if "category" in product:
        clean_category = product["category"].replace(" ", "_").replace("-", "_")
        base_tags["product_category"] = clean_category

    @asset(
        name=f"{asset_name_base}_plan",
        partitions_def=build_partition_def,
        group_name=f"build_{asset_name_base}",
        tags={**base_tags, "build_stage": "plan"},
    )
    def plan_asset(context: AssetExecutionContext, local_storage: LocalStorageResource):
        partition_key = context.partition_key
        output_path = local_storage.get_path(
            "builds", product_id, partition_key, "plan"
        )

        return MaterializeResult(
            metadata={"output_path": str(output_path), "stage": "plan"}
        )

    @asset(
        name=f"{asset_name_base}_load",
        partitions_def=build_partition_def,
        group_name=f"build_{asset_name_base}",
        ins={"plan": AssetIn(f"{asset_name_base}_plan")},
        tags={**base_tags, "build_stage": "load"},
    )
    def load_asset(
        context: AssetExecutionContext, local_storage: LocalStorageResource, plan
    ):
        partition_key = context.partition_key
        output_path = local_storage.get_path(
            "builds", product_id, partition_key, "load"
        )

        return MaterializeResult(
            metadata={"output_path": str(output_path), "stage": "load"}
        )

    @asset(
        name=f"{asset_name_base}_build",
        partitions_def=build_partition_def,
        group_name=f"build_{asset_name_base}",
        ins={"load": AssetIn(f"{asset_name_base}_load")},
        tags={**base_tags, "build_stage": "build", "executes_command": "true"},
    )
    def build_asset(
        context: AssetExecutionContext, local_storage: LocalStorageResource, load
    ):
        partition_key = context.partition_key
        output_path = local_storage.get_path(
            "builds", product_id, partition_key, "build"
        )

        result = subprocess.run(
            build_command, shell=True, capture_output=True, text=True, cwd=output_path
        )

        return MaterializeResult(
            metadata={
                "output_path": str(output_path),
                "stage": "build",
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        )

    @asset(
        name=f"{asset_name_base}_package",
        partitions_def=build_partition_def,
        group_name=f"build_{asset_name_base}",
        ins={"build": AssetIn(f"{asset_name_base}_build")},
        tags={**base_tags, "build_stage": "package"},
    )
    def package_asset(
        context: AssetExecutionContext, local_storage: LocalStorageResource, build
    ):
        partition_key = context.partition_key
        output_path = local_storage.get_path(
            "builds", product_id, partition_key, "package"
        )

        return MaterializeResult(
            metadata={"output_path": str(output_path), "stage": "package"}
        )

    # Add review asset to the build group - depends on package and shares same partitions
    @asset(
        name=f"{asset_name_base}_review",
        partitions_def=build_partition_def,
        group_name=f"build_{asset_name_base}",
        ins={"package": AssetIn(f"{asset_name_base}_package")},
        tags={**base_tags, "build_stage": "review"},
    )
    def review_asset(
        context: AssetExecutionContext, local_storage: LocalStorageResource, package
    ):
        partition_key = context.partition_key  # e.g., "2025.1.1-1_initial_build"
        output_path = local_storage.get_path("review", product_id, partition_key)

        # Now review operates on the same partition as the build pipeline

        return MaterializeResult(
            metadata={
                "output_path": str(output_path),
                "stage": "review",
                "build_version": partition_key,
                "note": f"Review of build {partition_key}",
            }
        )

    return [
        plan_asset,
        load_asset,
        build_asset,
        package_asset,
        review_asset,
    ], build_partition_def


def make_distribute_asset_group(product: Dict[str, Any]):
    """Create distribution assets for each product to multiple destinations"""
    product_id = product["id"]
    asset_name_base = product_id.replace(" ", "_").replace("-", "_")

    # Distribution uses major.minor partitions like review
    distribute_partition_def = DynamicPartitionsDefinition(
        name=f"distribute_{asset_name_base}"
    )

    # Create base tags with product metadata (clean values for Dagster tag restrictions)
    base_distribute_tags = {
        "product": asset_name_base,
        "product_id": asset_name_base,  # Use cleaned name instead of original
        "lifecycle_stage": "distribute",
        "destination_count": "3",  # socrata, bytes, ftp
    }

    # Add optional product metadata (clean for Dagster tag compliance)
    if "description" in product:
        cleaned_description = re.sub(r"[^a-zA-Z0-9_.-]", "_", product["description"])[
            :63
        ]
        base_distribute_tags["product_description"] = cleaned_description
    if "category" in product:
        cleaned_category = re.sub(r"[^a-zA-Z0-9_.-]", "_", product["category"])
        base_distribute_tags["product_category"] = cleaned_category

    destinations = ["socrata", "bytes", "ftp"]
    distribute_assets = []

    for destination in destinations:

        def make_distribute_asset(dest):
            @asset(
                name=f"distribute_{asset_name_base}_{dest}",
                partitions_def=distribute_partition_def,
                group_name=f"distribute_{asset_name_base}",
                tags={
                    **base_distribute_tags,
                    "destination": dest,
                    "destination_type": "api"
                    if dest == "socrata"
                    else "storage"
                    if dest in ["bytes", "ftp"]
                    else "unknown",
                    "is_public_api": "true" if dest == "socrata" else "false",
                },
            )
            def distribute_asset(
                context: AssetExecutionContext, local_storage: LocalStorageResource
            ):
                partition_key = context.partition_key  # e.g., "2025.1"
                output_path = local_storage.get_path(
                    "distribute", product_id, partition_key, dest
                )

                return MaterializeResult(
                    metadata={
                        "output_path": str(output_path),
                        "stage": "distribute",
                        "version": partition_key,
                        "destination": dest,
                        "note": f"Distributes {product_id} v{partition_key} to {dest}",
                    }
                )

            return distribute_asset

        distribute_assets.append(make_distribute_asset(destination))

    return distribute_assets, distribute_partition_def


ingest_template_ids = get_ingest_template_ids()

# Build mapping from ingest datasets to products that use them
ingest_to_products_mapping = build_ingest_to_products_mapping()

# Create ingest assets with product tags
ingest_assets = []
for template_id in ingest_template_ids:
    using_products = ingest_to_products_mapping.get(template_id, [])
    ingest_assets.append(make_ingest_asset(template_id, using_products))

products = load_products()
build_asset_groups = []
distribute_asset_groups = []

for product in products:
    # Build assets (now includes review as 5th stage sharing same partitions)
    build_assets, _ = make_build_asset_group(product)
    build_asset_groups.append(build_assets)

    # Distribution assets (3 destinations per product)
    distribute_assets, _ = make_distribute_asset_group(product)
    distribute_asset_groups.append(distribute_assets)
