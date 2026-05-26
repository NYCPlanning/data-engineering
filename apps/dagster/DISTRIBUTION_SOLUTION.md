# Distribution Assets - Implementation Solution

## Overview

This document outlines how to dynamically generate distribution assets in Dagster based on product metadata.

## Product Metadata Structure

### Key Classes

From `dcpy.product_metadata.models.metadata`:

- **`OrgMetadata`**: Root organization metadata
  - Located at repo root (configured via `config.CONF["product_metadata"]["repo_path"]`)
  - Lists all products in `metadata.products`

- **`ProductMetadata`**: Per-product metadata
  - Retrieved via `org.product(product_name)`
  - Has method `all_destinations()` that returns list of destinations

- **`Destination`**: Individual publishing destination
  - Has `id`, `type`, `tags`, `custom` dict

### Loading Product Metadata

```python
from dcpy.lifecycle import product_metadata

# Load organization metadata (with template vars if needed)
org = product_metadata.load(version='26v1', **other_template_vars)

# Get a specific product
product = org.product('pluto')

# Get all destinations for that product
destinations = product.all_destinations()
```

### Destination Structure

Each destination dict contains:

```python
{
    "product": "building_elevation_and_subgrade",
    "dataset_id": "building_elevation_and_subgrade",
    "destination_id": "socrata",
    "destination_type": "open_data",  # Connector type
    "tags": set(),
    "custom": {"four_four": "bsin-59hv"},  # Destination-specific config
    "destination_path": "building_elevation_and_subgrade.building_elevation_and_subgrade.socrata"
}
```

**Key Fields:**
- `product`: Product name (e.g., "pluto", "edde")
- `dataset_id`: Dataset within product
- `destination_id`: Unique destination identifier
- `destination_type`: Connector type (e.g., "open_data", "bytes", "socrata")
- `custom`: Dict with destination-specific configuration (e.g., Socrata four-four)
- `destination_path`: Unique path in format `{product}.{dataset}.{destination_id}`

---

## Dagster Implementation Strategy

### 1. Asset Generation Approach

We'll dynamically generate one Dagster asset **per destination** for each product.

**Asset naming pattern:**
```
{product}_dist_{dataset_id}_{destination_id}
```

Examples:
- `pluto_dist_pluto_socrata`
- `pluto_dist_pluto_water_included_socrata`
- `building_elevation_dist_building_elevation_socrata`

### 2. Implementation in `builds/assets.py`

```python
from dcpy.lifecycle import product_metadata, lifecycle

def make_distribution_assets_for_product(product: lifecycle.asset_models.Product):
    """Generate distribution assets for a single product.

    Returns a list of Dagster assets - one per destination.
    """
    # Load product metadata
    # NOTE: Template vars might need to come from partition context
    org = product_metadata.load()
    product_md = org.product(product.name)

    # Get all destinations for this product
    destinations = product_md.all_destinations()

    distribution_assets = []

    for dest in destinations:
        asset_func = make_single_distribution_asset(product, dest)
        distribution_assets.append(asset_func)

    return distribution_assets


def make_single_distribution_asset(product, destination: dict):
    """Create a single distribution asset for a specific destination."""

    # Generate unique asset name
    asset_name = f"{product.name}_dist_{destination['dataset_id']}_{destination['destination_id']}"

    # Determine upstream dependency
    # This should be external_review for the product
    upstream_asset = f"{product.name}_external_review"

    @asset(
        name=asset_name,
        group_name=product.name,  # Keep in product group for lineage
        partitions_def=build_partition_def,
        deps=[upstream_asset],
        tags={
            "product": product.name,
            "lifecycle_stage": "dist.publish",
            "domain": "distribution",
            "destination_type": destination["destination_type"],
            "destination_id": destination["destination_id"],
        },
    )
    def _distribution_asset(context: AssetExecutionContext):
        """Publish to {destination_type}: {destination_id}"""

        partition_key = context.partition_key

        context.log.info(
            f"Publishing {product.name} to {destination['destination_type']}: {destination['destination_id']}"
        )

        # Get packaged artifacts from previous step
        # These should be in blob storage from package_artifacts step
        artifacts = get_packaged_artifacts(
            product=product.name,
            version=partition_key
        )

        # Get the appropriate connector based on destination_type
        from dcpy.lifecycle.connector_registry import connectors
        connector = connectors[destination["destination_type"]]

        # Publish using connector
        # The connector interface varies by type, so we might need type-specific handling
        result = publish_to_destination(
            connector=connector,
            destination=destination,
            artifacts=artifacts,
            partition_key=partition_key
        )

        return MaterializeResult(
            metadata={
                "destination_type": destination["destination_type"],
                "destination_id": destination["destination_id"],
                "destination_path": destination["destination_path"],
                "published_files": [f.name for f in artifacts],
                "custom_config": destination.get("custom", {}),
                "publish_result": str(result),
            }
        )

    return _distribution_asset


def publish_to_destination(connector, destination: dict, artifacts, partition_key: str):
    """Handle publishing to a destination.

    Different destination types may have different connector interfaces.
    """
    destination_type = destination["destination_type"]
    destination_id = destination["destination_id"]
    custom_config = destination.get("custom", {})

    # Type-specific publishing logic
    if destination_type == "open_data":
        # Socrata/Open Data publishing
        # custom config might have "four_four" identifier
        four_four = custom_config.get("four_four")
        return connector.push(
            dataset_id=four_four,
            files=artifacts,
            version=partition_key
        )

    elif destination_type == "bytes":
        # EDM Bytes publishing
        return connector.push(
            key=destination_id,
            files=artifacts,
            version=partition_key
        )

    else:
        # Generic fallback
        return connector.push(
            key=destination_id,
            files=artifacts,
            version=partition_key
        )


# Generate distribution assets for all products
products = lifecycle.list_products()
distribution_assets = []
for product in products:
    distribution_assets.extend(make_distribution_assets_for_product(product))

# Add to build_assets export
build_assets = plan_recipe_assets + load_data_assets + distribution_assets
```

### 3. Key Challenges & Solutions

#### Challenge 1: Template Variables

**Problem:** Product metadata uses Jinja2 templates that need variables (e.g., `version`, `BUILD_ENGINE_VERSION`)

**Solution:** Pass partition key and other context as template vars when loading metadata:

```python
def make_distribution_assets_for_product(product):
    # In asset definition, not at module load time
    @asset(...)
    def _distribution_asset(context):
        partition_key = context.partition_key

        # Load metadata with template vars from context
        org = product_metadata.load(
            version=partition_key,
            BUILD_ENGINE_VERSION=partition_key,
            **other_vars_from_recipe
        )
```

**Alternative:** Load metadata at asset materialization time (not module import time) to have access to partition context.

#### Challenge 2: Product-Dataset-Destination Hierarchy

**Problem:** Destination paths are `{product}.{dataset}.{destination_id}` - one product can have multiple datasets, each with multiple destinations.

**Solution:**
- Asset name includes dataset: `{product}_dist_{dataset}_{destination_id}`
- For products with one dataset (common case), this is simple
- For products with multiple datasets, we get multiple sets of distribution assets

**Example:**
```
pluto_dist_pluto_socrata
pluto_dist_pluto_bytes
pluto_dist_pluto_clipped_socrata
pluto_dist_pluto_clipped_bytes
```

#### Challenge 3: Connector Interface Differences

**Problem:** Different destination types use different connectors with different interfaces.

**Solution:** Create a dispatch function that handles type-specific publishing:

```python
def publish_to_destination(connector, destination, artifacts, partition_key):
    """Central dispatch for different destination types."""
    # See implementation above
```

**Better Solution:** Standardize connector interface (separate task, not blocking).

---

## Testing Strategy

### 1. Verify Asset Generation

```python
# In a test or notebook
from dagster import build_asset_context
from apps.dagster.builds.assets import distribution_assets

# Check that assets are generated
print(f"Generated {len(distribution_assets)} distribution assets")

# Check asset names
for asset in distribution_assets[:5]:
    print(asset.name)
```

### 2. Mock Materialization

```python
# Test a single distribution asset without actually publishing
context = build_asset_context()
result = pluto_dist_pluto_socrata(context)
print(result.metadata)
```

### 3. Integration Test

```bash
# In Dagster UI
dagster dev

# Navigate to pluto group
# Verify distribution assets appear
# Check lineage shows: external_review → [dist_1, dist_2, dist_3]
```

---

## Migration Path

### Phase 1: Generate Assets (No Publishing)

1. Implement `make_distribution_assets_for_product()`
2. Generate assets that log what they would do
3. Verify in Dagster UI that assets appear correctly
4. Check lineage graph shows fan-out pattern

### Phase 2: Implement Publishing

1. Implement `publish_to_destination()` for one destination type (e.g., bytes)
2. Test with a single product
3. Add error handling and logging
4. Expand to other destination types

### Phase 3: Handle Edge Cases

1. Products with no destinations (skip distribution asset generation)
2. Products with multiple datasets
3. Template variable resolution from recipe/partition context
4. Connector interface standardization

---

## Open Questions

### 1. Template Variables for Product Metadata

**Question:** Where do template vars come from for `product_metadata.load()`?

**Options:**
- A: From partition key (e.g., `version='26v1'`)
- B: From recipe.yml template vars (same as plan step)
- C: Both

**Recommendation:** Start with partition key only, expand if needed.

### 2. Dependency on Package vs. External Review

**Question:** Should distribution depend on `package_artifacts` or `external_review`?

**Current Spec:** `external_review` (allows skipping package step)

**Consideration:** If package is optional, distribution must handle both:
- With package: Get artifacts from `package_artifacts` upload location
- Without package: Get artifacts from `build_artifacts` upload location

**Solution:** Check if `package_artifacts` exists for product, use that if present, otherwise use `build_artifacts`.

### 3. Products Without Destinations

**Question:** What if a product has no destinations in metadata?

**Answer:** Skip distribution asset generation entirely. Not all products are published externally.

**Implementation:**
```python
destinations = product_md.all_destinations()
if not destinations:
    return []  # No distribution assets for this product
```

### 4. Connector Registration

**Question:** Are all destination types already registered in `connectors` registry?

**Answer:** Need to verify. Check `dcpy.lifecycle.connector_registry`.

**Fallback:** If connector not found, log warning and skip (or fail asset materialization).

---

## File Organization

```
apps/dagster/builds/
├── assets.py              # Build assets + distribution asset generators
├── distribution.py        # NEW: Distribution-specific helpers
│   ├── make_distribution_assets_for_product()
│   ├── make_single_distribution_asset()
│   └── publish_to_destination()
├── partitions.py
├── resources.py
└── definitions.py
```

**Recommendation:** Keep distribution logic in `assets.py` initially, extract to `distribution.py` if it grows large.

---

## Next Steps

1. ✅ **Read and understand product metadata structure** (DONE)
2. **Implement asset generation** in `builds/assets.py`:
   - `make_distribution_assets_for_product()`
   - `make_single_distribution_asset()`
3. **Test asset generation** (no publishing yet):
   - Run `dagster dev`
   - Verify assets appear in UI
   - Check lineage graph
4. **Implement publishing logic**:
   - Start with `bytes` destination type
   - Add error handling
   - Add logging
5. **Handle edge cases**:
   - Products without destinations
   - Multiple datasets per product
   - Template variable resolution
6. **Update SPEC.md** with implementation details
