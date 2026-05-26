# Dagster Application Specification

**Version:** 0.4
**Last Updated:** 2026-05-14
**Status:** Draft - Evolving Design Document

## Overview

This Dagster application manages three primary domains for the data engineering pipeline:

1. **Ingest**: Regularly pulling data from external sources, normalizing it, and storing in blob storage
2. **Builds**: Transforming ingested data through planning, loading, processing (dbt/Python), QA, and packaging
3. **Distribution**: Publishing build artifacts to external destinations (Socrata, S3, etc.)

## Design Principles

- **Asset-based thinking**: Think in nouns (states) rather than verbs (actions)
- **Idempotency**: Steps should be safely re-runnable
- **Product-centric**: All assets tagged and organized by product (except for ingest)
- **Partitioned workflows**: Each domain uses partitions to track versions/time periods
- **Separation of concerns**: Build process (ephemeral) vs. Build artifacts (durable)

---

## 1. Ingest Domain

**Status:** ✅ Implemented

Handles regular data pulls from external sources and normalization for downstream builds.

*(Details TBD - this is already implemented)*

---

## 2. Builds Domain

**Status:** 🚧 In Progress

### Overview

The builds domain transforms ingested data into publishable products through a series of steps.

### Partitioning Strategy

- **Partition Definition**: `build_partition_def` (shared across all build assets)
- **Partition Key Format**: Product-specific (e.g., "2024Q1", "26v1")
- **Scope**: All build steps for a given product share the same partition key

### Asset Naming Convention

**Simplified:** Remove redundant `builds_` prefix since Dagster groups by `group_name`

```
{product}_{step}
```

Examples:
- `edde_plan`
- `edde_source_data`
- `edde_build`
- `edde_build_artifacts`
- `pluto_qa`
- `pluto_external_review`

### Asset Organization

**Group by Product:**
- Each product gets its own group: `group_name="{product}"`
- Clicking product group in UI shows full end-to-end lineage (plan → distribution)
- Ingest domain uses separate `group_name="ingest"` (not product-centric)

**Asset Configuration:**
```python
@asset(
    name="pluto_plan",
    group_name="pluto",  # Product-based grouping
    partitions_def=build_partition_def,
    tags={
        "product": "pluto",
        "lifecycle_stage": "builds.plan",
        "domain": "builds"
    }
)
```

**Jobs:**
- Define per-product jobs for easy pipeline execution
- Example: `build_pluto_job`, `build_edde_job`
- Allows "run entire product pipeline" with one click

```python
pluto_job = define_asset_job(
    name="build_pluto",
    selection=AssetSelection.groups("pluto"),
    partitions_def=build_partition_def
)
```

### Build Types

Products can have different build types, configured in recipe.yml:

```yaml
stage_config:
  build:
    type: dbt | sql | python | bash
    # Optional: specify tables to export (for sql/dbt builds)
    export_tables:
      - name: pluto_output
        format: shapefile
      - name: pluto_csv
        format: parquet
```

**Build Type Execution:**
- **dbt**: Use Dagster's dbt integration - auto-generates assets per dbt model
- **python**: Dynamically import and execute Python module/function
- **sql**: Execute SQL scripts
- **bash**: Execute shell script (fallback)

**Python Module Configuration:**
```yaml
stage_config:
  build:
    type: python
    module: path.to.module  # e.g., "external_review.collate"
    function: run_build
    args:  # Optional arguments to pass to function
      arg1: value1
      arg2: value2
```

**Convention over Configuration:**
- Default: Each product has `build.sh` script in product directory
- Dagster calls: `./build.sh {stage}` where stage is: `build`, `qa`, `package`
- If stage not supported by build.sh, it should exit 0 (no-op)
- Override via recipe.yml `stage_config` if needed

### Build Steps

All steps are **linear and sequential**. Each step is a separate Dagster asset.

#### Step 1: Plan ✅ IMPLEMENTED

**Asset Name:** `{product}_plan`

**Materialized Artifact:** `recipe.lock.yml` uploaded to blob storage (plan connector)

**Purpose:** Resolve recipe templates and dataset versions

**Implementation:**
- Takes template variables as config (dynamically generated from recipe.yml)
- Resolves all `{{ vars }}` and dataset versions to specific values
- Validates that referenced dataset versions exist in ingest storage
- Creates `recipe.lock.yml` with fully resolved configuration
- Uploads plan to blob storage via plan connector

**Metadata:**
- Template variables used
- Plan upload location
- Partition key

**Dependencies:** None (first step)

---

#### Step 2: Source Data 🚧 IN PROGRESS

**Asset Name:** `{product}_source_data`

**Materialized Artifact:** `build_metadata.json` documenting loaded datasets

**Purpose:** Ensure source data from ingest is loaded into build environment (postgres/filesystem)

**Implementation:**
- Pull `recipe.lock.yml` from plan storage
- Call `dcpy.lifecycle.builds.load.load_source_data_from_resolved_recipe()`
- Load datasets into postgres schema and/or local filesystem per recipe configuration
- **Idempotent**: Dagster handles re-materialization

**Behavior:**
- **Fails immediately** if dataset version is missing
- **For dbt/sql builds only**: Ensure postgres schema exists (don't drop if already exists)
  - Check if schema exists before calling `setup_build_pg_schema()`
  - Skip schema setup if already exists (preserves existing data)
- Imports datasets according to `destination` field (postgres/file/df)

**Side Effects (not materialized assets):**
- **For dbt/sql builds**: Datasets in postgres schema `{build_name}` (temporary/scratch space)
- **For python/bash builds**: Datasets in local data library (if file-based)

**Metadata:**
- Build type (determines postgres usage)
- Build schema name (postgres, if applicable)
- Number of datasets loaded
- Dataset IDs and versions

**Dependencies:** `{product}_plan`

---

#### Step 3: Build ⏳ NOT IMPLEMENTED

**Asset Name:** `{product}_build`

**Materialized Artifact:** Build report (logs, performance stats, table metadata)

**Purpose:** Execute build transformations to create output tables/files

**Implementation:**

Based on `stage_config.build.type`:

**For dbt builds:**
```python
@dbt_assets(manifest=product.dbt_manifest)
def {product}_dbt_models(context):
    """Auto-generated assets per dbt model."""
    yield from dbt.cli(["build"]).stream()

@asset(name="{product}_build", deps=[{product}_dbt_models])
def {product}_build_report(context):
    """Aggregate build report from dbt run."""
    results = parse_dbt_run_results()
    return MaterializeResult(
        metadata={
            "build_type": "dbt",
            "models_built": results.models,
            "test_results": results.tests,
            "duration_seconds": results.duration
        }
    )
```

**For python/sql/bash builds:**
```python
@asset(name="{product}_build")
def {product}_build(context, {product}_source_data):
    """Execute build transformation."""
    start_time = time.time()

    # Execute based on type
    if build_type == "python":
        # Dynamically import module and function
        module_path = recipe.stage_config.build.module
        function_name = recipe.stage_config.build.function
        args = recipe.stage_config.build.get("args", {})

        module = importlib.import_module(module_path)
        func = getattr(module, function_name)
        result = func(context, **args)
    else:
        result = subprocess.run(["./build.sh", "build"])

    duration = time.time() - start_time

    return MaterializeResult(
        metadata={
            "build_type": build_type,
            "duration_seconds": duration,
            "tables_created": [...],  # For postgres builds
            "files_created": [...],   # For file builds
            "build_log": "path/to/build.log"
        }
    )
```

**Side Effects (not materialized assets):**
- **Postgres builds**: Tables in build schema (temporary/scratch space)
- **File builds**: Output files in build directory

**Storage:**
- **Build reports MUST use private bucket** (contains performance stats, internal metadata)
- Use private connector for uploading build logs/reports
- Unlike most storage which is public, build telemetry is internal-only

**Metadata:**
- Build type
- Duration
- Tables/files created (listing only, not the artifacts themselves)
- Build log path
- Success/failure status per step

**Dependencies:** `{product}_source_data`

---

#### Step 4: Build Artifacts (Export) ⏳ NOT IMPLEMENTED

**Asset Name:** `{product}_build_artifacts`

**Materialized Artifact:** Dataset files (shapefiles, parquet, CSV, etc.) uploaded to cloud storage

**Purpose:** Export build outputs to files and upload to cloud

**Implementation:**

```python
@asset(name="{product}_build_artifacts")
def {product}_build_artifacts(context, {product}_build):
    """Export build outputs to files and upload."""

    build_meta = context.upstream_output.metadata
    export_config = recipe.stage_config.build.exports

    if build_meta["build_type"] in ["dbt", "sql"]:
        # Export specified tables from postgres to files
        files = export_tables_from_postgres(
            schema=build_meta.get("pg_schema"),
            datasets=export_config.datasets,  # List of {name, filename, format}
            output_folder=export_config.output_folder
        )
    else:  # build_type == "python" or "bash" (files already exist)
        # Collect the files from build directory
        files = collect_build_outputs(
            build_directory=build_path,
            output_folder=export_config.output_folder
        )

    # Upload to cloud storage (builds connector)
    upload_result = upload_to_cloud(
        files=files,
        connector=get_builds_default_connector(),
        key=product_name,
        version=partition_key
    )

    return MaterializeResult(
        metadata={
            "exported_files": [f.name for f in files],
            "file_sizes_mb": {f.name: f.size for f in files},
            "upload_location": upload_result.location,
            "total_size_mb": sum(f.size for f in files)
        }
    )
```

**Export Configuration (recipe.yml):**

Based on existing convention (see `products/cscl/recipe.yml`):

```yaml
stage_config:
  build:
    type: sql  # or dbt
    exports:
      output_folder: output
      datasets:
        - name: manhattan_lion_dat      # Table name in postgres
          filename: ManhattanLION.dat   # Output filename
          format: dat                    # Output format
        - name: pluto_output
          filename: pluto_26v1.shp
          format: shapefile
        - name: pluto_csv
          filename: pluto_26v1.parquet
          format: parquet
```

**Notes:**
- `custom` field (e.g., `custom: { formatting: lion_dat }`) will be implemented later
- For now, standard format conversion only

**Behavior:**
- **dbt/sql builds**: Export specified tables to configured formats
- **Python/bash builds**: Collect files from output folder
- Upload to builds connector (blob storage) at path: `{product}/builds/{version}/`
- **This is the first durable artifact** - files persist beyond build lifetime

**Retry Strategy:**
- Can retry export independently without re-running build
- Build report still exists, postgres tables may still exist (if not cleaned up)

**Dependencies:** `{product}_build`

---

#### Step 5: QA ⏳ NOT IMPLEMENTED

**Asset Name:** `{product}_qa`

**Materialized Artifact:** QA report (validation results, test outputs) - uploaded to PRIVATE bucket

**Purpose:** Run quality assurance checks and validations

**Implementation:**

```python
@asset(name="{product}_qa")
def {product}_qa(context, {product}_build_artifacts):
    """Run QA validation."""
    start_time = time.time()

    qa_config = recipe.stage_config.get("qa", {})

    if qa_config.get("type") == "python":
        # Dynamically import module and function
        module = importlib.import_module(qa_config["module"])
        func = getattr(module, qa_config["function"])
        result = func(context, **qa_config.get("args", {}))
    else:
        result = subprocess.run(["./build.sh", "qa"])

    duration = time.time() - start_time

    # Upload QA report to PRIVATE bucket
    upload_qa_report(result, private_connector)

    return MaterializeResult(
        metadata={
            "qa_passed": result.success,
            "validations_run": result.validation_count,
            "failures": result.failures,
            "qa_log": "path/to/qa.log",
            "duration_seconds": duration
        }
    )
```

**Configuration:**
```yaml
stage_config:
  qa:
    enabled: true  # Optional - if false, skip asset creation entirely
    type: python
    module: qa.validate
    function: run_qa
    args:
      threshold: 0.95
```

**Optional:** If `stage_config.qa.enabled` is false or missing, do not create this asset

**QA Outputs:**
- Can produce validation tables in postgres
- Can produce QA reports on filesystem
- May fail the asset if validations fail (blocking downstream steps)

**Storage:**
- **QA reports MUST use private bucket** (internal validation details)

**Dependencies:** `{product}_build_artifacts`

---

#### Step 6: QA Artifacts ⏳ NOT IMPLEMENTED

**Asset Name:** `{product}_qa_artifacts`

**Materialized Artifact:** QA output files (CSVs, validation reports) uploaded to build storage

**Purpose:** Export and upload QA validation outputs for review

**Implementation:**

```python
@asset(name="{product}_qa_artifacts")
def {product}_qa_artifacts(context, {product}_qa):
    """Export QA outputs and upload to build storage."""

    qa_config = recipe.stage_config.get("qa", {})
    export_config = qa_config.get("exports", {})

    # Collect QA output files (mostly CSVs and text files)
    qa_files = collect_qa_outputs(
        output_folder=export_config.get("output_folder", "qaqc"),
        datasets=export_config.get("datasets", [])
    )

    # Upload to builds connector in a qaqc subfolder
    # Path: {product}/builds/{version}/qaqc/
    upload_result = upload_to_cloud(
        files=qa_files,
        connector=get_builds_default_connector(),
        key=product_name,
        version=partition_key,
        subfolder="qaqc"
    )

    return MaterializeResult(
        metadata={
            "qa_files": [f.name for f in qa_files],
            "file_sizes_mb": {f.name: f.size for f in qa_files},
            "upload_location": upload_result.location,
            "total_size_mb": sum(f.size for f in qa_files)
        }
    )
```

**Configuration (recipe.yml):**
```yaml
stage_config:
  qa:
    enabled: true
    type: python
    module: qa.validate
    function: run_qa
    exports:
      output_folder: qaqc
      datasets:
        - name: validation_results
          filename: validation_results.csv
          format: csv
        - name: qa_summary
          filename: qa_summary.txt
          format: txt
```

**Storage Location:**
- Files uploaded to: `{product}/builds/{version}/qaqc/`
- Example: `pluto/builds/26v1/qaqc/validation_results.csv`

**Optional:** If no QA or no QA exports configured, skip this asset

**Dependencies:** `{product}_qa`

---

#### Step 7: Package Artifacts ⏳ NOT IMPLEMENTED

**Asset Name:** `{product}_package_artifacts`

**Materialized Artifact:** Packaged/enhanced dataset files ready for distribution

**Purpose:** Enhance exported files with metadata, READMEs, data dictionaries

**Implementation:**

```python
@asset(name="{product}_package_artifacts")
def {product}_package_artifacts(context, {product}_qa_artifacts, {product}_build_artifacts):
    """Package artifacts for distribution."""

    build_artifacts_meta = context.upstream_output.metadata_for("{product}_build_artifacts")
    exported_files = get_exported_files(build_artifacts_meta["upload_location"])

    package_config = recipe.stage_config.get("package", {})

    if package_config.get("type") == "python":
        # Dynamically import module and function
        module = importlib.import_module(package_config["module"])
        func = getattr(module, package_config["function"])
        packaged_files = func(context, exported_files, **package_config.get("args", {}))
    else:
        subprocess.run(["./build.sh", "package"])
        packaged_files = collect_packaged_outputs()

    return MaterializeResult(
        metadata={
            "packaged_files": [f.name for f in packaged_files],
            "enhancements_applied": ["readme", "data_dict", "metadata_stamp"],
            "package_location": "..."
        }
    )
```

**Configuration:**
```yaml
stage_config:
  package:
    enabled: true
    type: python
    module: package.enhance
    function: run_package
    args:
      include_readme: true
```

**Optional:** If `stage_config.package.enabled` is false or missing, skip this asset

**Operations:**
- Pull README, data dictionary from product metadata
- Stamp metadata into geospatial formats (shapefiles, FGDB)
- Create ZIP archives if needed (defer to later - CSCL implementation)
- Generate manifest files
- Re-upload enhanced artifacts

**Dependencies:**
- `{product}_qa_artifacts` (if QA artifacts exist)
- `{product}_build_artifacts` (always)

---

#### Step 8: External Review ⏳ NOT IMPLEMENTED

**Asset Name:** `{product}_external_review`

**Materialized Artifact:** None (approval gate only)

**Purpose:** Manual approval gate before distribution

**Implementation:**

```python
@asset(name="{product}_external_review")
def {product}_external_review(context, {product}_package):
    """Manual approval gate - requires manual materialization."""

    # This asset does nothing except serve as a dependency gate
    # Must be manually materialized by external QA team

    context.log.info("External review completed - approved for distribution")

    return MaterializeResult(
        metadata={
            "approved_by": "external_qa_team",
            "approval_timestamp": datetime.now().isoformat()
        }
    )
```

**Behavior:**
- **Requires manual materialization** - cannot be auto-triggered
- Blocks all distribution assets until approved
- Future: Could integrate with external approval workflow system

**Lifecycle Stage:** `builds.external_review` (part of builds, not distribution)

**Dependencies:**
- `{product}_package_artifacts` (if package exists)
- `{product}_qa_artifacts` (if no package but QA exists)
- `{product}_build_artifacts` (if no QA or package)

---

### Build Step Summary

**Simplified Linear Flow:**

```
{product}_plan
    ↓
{product}_source_data
    ↓
{product}_build                ← Materialized: build report (PRIVATE storage)
    ↓
{product}_build_artifacts      ← Materialized: dataset files (public storage)
    ↓
{product}_qa                   ← Materialized: QA report (PRIVATE storage) [optional]
    ↓
{product}_qa_artifacts         ← Materialized: QA output files in qaqc/ subfolder [optional]
    ↓
{product}_package_artifacts    ← Materialized: enhanced files [optional]
    ↓
{product}_external_review      ← Manual gate
```

**For dbt products:**

```
{product}_plan
    ↓
{product}_source_data
    ↓
{product}_{dbt_model_1}        ← Auto-generated dbt assets
{product}_{dbt_model_2}
{product}_{dbt_model_3}
    ↓
{product}_build                ← Aggregate dbt report
    ↓
{product}_build_artifacts      ← Export dbt outputs to files
    ↓
{product}_qa                   ← Run QA validations
    ↓
{product}_qa_artifacts         ← Export QA outputs
    ↓
{product}_package_artifacts    ← Enhance with metadata/READMEs
    ↓
{product}_external_review
```

---

## 3. Distribution Domain

**Status:** ⏳ Not Implemented

### Overview

Distribution publishes build artifacts to external destinations (Socrata, S3, EDM, etc.).

### Partitioning Strategy

- **Partition Definition**: Same as builds (`build_partition_def`)
- **Rationale**: Straight shot from build → distribution for a given version
- **Consideration**: Final destinations often don't have versions (e.g., only one PLUTO on opendata)

### Asset Naming Convention

```
{product}_dist_{destination_key}
```

Examples:
- `pluto_dist_socrata_main`
- `pluto_dist_socrata_water_included`
- `edde_dist_s3_public`

### Distribution Implementation

**One asset per destination** - all run in **parallel** (fan-out from external_review)

```python
@asset(
    name="{product}_dist_{destination_key}",
    group_name="{product}",  # Include in product group for full lineage view
    partitions_def=build_partition_def,
    tags={
        "product": "{product}",
        "lifecycle_stage": "dist.publish",
        "domain": "distribution",
        "destination_type": "{destination_type}"
    }
)
def {product}_dist_{destination_key}(context, {product}_external_review):
    """Publish to specific destination."""

    # Read product metadata for destination config
    dest_config = get_destination_config(product, destination_key)

    # Get packaged artifacts from previous step
    artifacts = get_packaged_artifacts(product, partition_key)

    # Use existing connector infrastructure
    connector = connectors[dest_config["destination-type"]]
    result = connector.push(
        key=dest_config["destination-key"],
        files=artifacts,
        **dest_config.get("connector_args", {})
    )

    return MaterializeResult(
        metadata={
            "destination_type": dest_config["destination-type"],
            "destination_key": dest_config["destination-key"],
            "published_files": artifacts,
            "publish_result": result
        }
    )
```

**Destination Configuration:**
- **Source**: `dcpy/product_metadata/{product}/publishing.yml`
- **Fields**:
  - `destination-type`: Connector type (e.g., "socrata", "edm.bytes")
  - `destination-key`: Specific destination identifier

**Example (PLUTO with 2 Socrata destinations):**

Product metadata (`dcpy/product_metadata/pluto/publishing.yml`):
```yaml
destinations:
  - destination-type: socrata
    destination-key: pluto-main
  - destination-type: socrata
    destination-key: pluto-water-included
```

Generated assets:
- `pluto_dist_socrata_pluto_main`
- `pluto_dist_socrata_pluto_water_included`

Both run in parallel after `pluto_external_review` is approved.

**Dependencies:** `{product}_external_review`

---

### Distribution Step Summary

**Fan-out Pattern:** All distribution assets run in parallel after external review approval

```
{product}_external_review  ← Manual gate (builds domain)
    ↓
    ├─→ {product}_dist_socrata_dest1  ← Parallel
    ├─→ {product}_dist_socrata_dest2  ← Parallel
    └─→ {product}_dist_s3_dest3       ← Parallel
```

**Dagster Lineage View:**
- All distribution assets in same product group
- Shows fan-out from external_review node
- Can materialize all destinations at once or individually
- Clicking product group (e.g., "pluto") shows full pipeline from plan through all distribution destinations

---

## Cross-Cutting Concerns

### Materialized Assets vs. Side Effects

**Key Principle:** Distinguish between durable materialized assets and ephemeral side effects

**Materialized Assets (durable):**
- Plan: `recipe.lock.yml` in blob storage
- Source Data: `build_metadata.json`
- Build: Build report with performance stats (PRIVATE bucket)
- Build Artifacts: Dataset files (shapefiles, parquet, CSV) (PUBLIC bucket)
- QA: QA report (PRIVATE bucket)
- QA Artifacts: QA output files in qaqc/ subfolder (PUBLIC bucket)
- Package Artifacts: Enhanced dataset files (PUBLIC bucket)
- Distribution: Published artifacts

**Side Effects (ephemeral):**
- Postgres tables in build schema (scratch space, can be deleted after export)
- Intermediate files during build
- Build logs (unless explicitly uploaded)

**Philosophy:**
- Postgres tables are **working memory**, not the final product
- Export step creates **durable artifacts** from ephemeral build state
- Can clean up postgres schemas after export without "unmaterializing" assets
- Retry export without re-running build (if postgres tables still exist)

### Storage & Security

**Private vs. Public Storage:**

- **PRIVATE bucket** (internal use only):
  - Build reports (performance stats, internal metadata)
  - QA reports (validation details)
  - Build logs
  - Any telemetry or operational data

- **PUBLIC bucket** (external access):
  - Exported dataset files
  - Packaged artifacts
  - Distribution-ready files

**Implementation:**
- Use separate connectors for private vs. public storage
- Default most storage to public, explicitly configure private for build/QA outputs

### Error Handling

**Philosophy:** Fail fast, leave artifacts for debugging

**Behavior on Failure:**
- ❌ **No automatic cleanup** of postgres schemas or partial artifacts
- ✅ **Leave artifacts in place** for manual inspection and debugging
- Dagster's retry mechanisms available for transient failures

**Rationale:**
- Initial deployment is single-machine (server + runner)
- MVP phase prioritizes debuggability over automation
- Manual intervention acceptable in early stages

### Tagging & Grouping Strategy

**Primary Organization: Product Groups**
- Most assets grouped by product: `group_name="{product}"`
- Ingest assets use: `group_name="ingest"`
- Provides full lineage view in Dagster UI when clicking product group

**Required Tags:**
- `product`: Product name (e.g., "pluto", "edde") - for all builds/distribution assets
- `lifecycle_stage`: Stage identifier (e.g., "builds.plan", "builds.build_artifacts", "dist.publish")
- `domain`: High-level domain (e.g., "builds", "distribution", "ingest")

**Optional Tags:**
- `destination_type`: For distribution assets (e.g., "socrata", "edm.bytes")
- `build_type`: For build assets (e.g., "dbt", "python", "sql")

**Examples:**

```python
# Build asset
@asset(
    name="edde_build_artifacts",
    group_name="edde",  # Product group
    partitions_def=build_partition_def,
    tags={
        "product": "edde",
        "lifecycle_stage": "builds.build_artifacts",
        "domain": "builds"
    }
)

# Distribution asset
@asset(
    name="pluto_dist_socrata_main",
    group_name="pluto",  # Same product group for full lineage
    partitions_def=build_partition_def,
    tags={
        "product": "pluto",
        "lifecycle_stage": "dist.publish",
        "domain": "distribution",
        "destination_type": "socrata"
    }
)

# Ingest asset (not product-specific)
@asset(
    name="ingest_dcp_pluto",
    group_name="ingest",  # Separate ingest group
    tags={
        "domain": "ingest",
        "dataset": "dcp_pluto"
    }
)
```

**UI Benefits:**
- Click "pluto" group → see plan → build → qa → package → external_review → [dist_socrata_1, dist_socrata_2, ...] with full DAG lineage
- Filter by `tag:domain=builds` → see all build assets across products
- Filter by `tag:lifecycle_stage=dist.publish` → see all distribution assets
- Selection syntax: `pluto*` or `tag:product=pluto` for programmatic access

### Resource Usage

**Local Storage:**
- `LocalStorageResource`: Manages build directories
- Path structure: `/tmp/dagster/{domain}/{product}/{partition_key}/`

**Connectors:**
- Plan connector: `get_plan_default_connector()`
- Private connector: `get_private_default_connector()` (for build/QA reports)
- Builds connector: `get_builds_default_connector()` (for exported artifacts)
- Distribution connectors: Per destination type (from product metadata)

---

## Implementation Status

| Domain | Step | Status | Notes |
|--------|------|--------|-------|
| **Ingest** | All | ✅ Implemented | Pre-existing |
| **Builds** | Plan | ✅ Implemented | `{product}_plan` |
| **Builds** | Source Data | 🚧 In Progress | `{product}_source_data` |
| **Builds** | Build | ⏳ Not Started | `{product}_build` - build report to PRIVATE bucket |
| **Builds** | Build Artifacts | ⏳ Not Started | `{product}_build_artifacts` - export datasets |
| **Builds** | QA | ⏳ Not Started | `{product}_qa` - QA report to PRIVATE bucket [optional] |
| **Builds** | QA Artifacts | ⏳ Not Started | `{product}_qa_artifacts` - export QA outputs [optional] |
| **Builds** | Package | ⏳ Not Started | `{product}_package_artifacts` - enhance files [optional] |
| **Builds** | External Review | ⏳ Not Started | `{product}_external_review` - manual gate |
| **Distribution** | Publish | ⏳ Not Started | `{product}_dist_{dest}` - one per destination |

---

## TODOs

1. **Recipe syntax cleanup**: Change `stage_config.builds.*` → `stage_config.*` (assume recipes are build-specific)
   - Current: `stage_config.builds.build.type`
   - Future: `stage_config.build.type`
   - Note: Don't implement yet, just document for future refactor

2. **Private connector implementation**: Ensure private bucket connector exists for build/QA reports

3. **Build type implementations**: Create execution handlers for dbt, python, sql, bash types

4. **Product metadata reader**: Parse `dcpy/product_metadata/{product}/publishing.yml` for distribution destinations

5. **Per-product jobs**: Define Dagster jobs for easy pipeline execution
   ```python
   pluto_job = define_asset_job(
       name="build_pluto",
       selection=AssetSelection.groups("pluto"),
       partitions_def=build_partition_def
   )
   ```

---

## Open Questions & Future Considerations

1. **Postgres cleanup strategy**: When to delete build schemas? After export? After distribution? Manual?
2. **Build command dependencies**: For products with complex build steps, how to express dependencies?
3. **Partition retention**: How long to keep old build partitions? Automatic cleanup policy?
4. **Monitoring**: What metrics to track (step duration, artifact sizes, failure rates)?
5. **Approval workflow**: Future integration with formal approval system vs. manual materialization?
6. **Incremental builds**: Any products that could benefit from incremental processing?
7. **Build.sh convention enforcement**: Require build.sh to exist, or make truly optional?

---

## Revision History

| Date | Version | Changes |
|------|---------|---------|
| 2026-05-13 | 0.1 | Initial specification based on requirements discussion |
| 2026-05-14 | 0.2 | Simplified model: single asset per stage, build/export separation, private storage for build reports |
| 2026-05-14 | 0.3 | Refined export configuration (based on cscl convention), added QA artifacts asset, renamed export assets to *_artifacts pattern, added dynamic Python module imports with args, clarified postgres schema creation (ensure not create for dbt/sql) |
| 2026-05-14 | 0.4 | Added asset organization strategy: product-based groups for full lineage view, distribution fan-out pattern for parallel execution, detailed tagging strategy, per-product jobs |
