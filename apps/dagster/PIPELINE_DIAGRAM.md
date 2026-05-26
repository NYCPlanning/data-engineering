# Dagster Build Pipeline - Visual Overview

This diagram shows the complete asset lineage for a typical product in our Dagster implementation.

## Full Product Pipeline

```mermaid
graph TD
    %% Styling
    classDef planStyle fill:#e1f5ff,stroke:#0288d1,stroke-width:2px
    classDef buildStyle fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef qaStyle fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef packageStyle fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    classDef reviewStyle fill:#ffebee,stroke:#c62828,stroke-width:3px
    classDef distStyle fill:#e0f2f1,stroke:#00796b,stroke-width:2px

    %% Plan Stage
    plan["product_plan<br/><i>Resolve recipe & versions</i>"]:::planStyle

    %% Load Stage
    source["product_source_data<br/><i>Load datasets into postgres/files</i>"]:::planStyle

    %% Build Stage
    build["product_build<br/><i>Execute transformations</i><br/>📊 Build report → PRIVATE bucket"]:::buildStyle
    build_artifacts["product_build_artifacts<br/><i>Export datasets to files</i><br/>📁 Shapefiles, Parquet, CSV → PUBLIC bucket"]:::buildStyle

    %% QA Stage (Optional)
    qa["product_qa<br/><i>Run validations</i><br/>📊 QA report → PRIVATE bucket<br/><small>(optional)</small>"]:::qaStyle
    qa_artifacts["product_qa_artifacts<br/><i>Export QA outputs</i><br/>📁 CSV/TXT → qaqc/ subfolder<br/><small>(optional)</small>"]:::qaStyle

    %% Package Stage (Optional)
    package["product_package_artifacts<br/><i>Add READMEs, metadata</i><br/>📦 Enhanced files → PUBLIC bucket<br/><small>(optional)</small>"]:::packageStyle

    %% External Review (Manual Gate)
    review["product_external_review<br/><i>Manual approval gate</i><br/>⏸️ Requires manual materialization"]:::reviewStyle

    %% Distribution (Fan-out)
    dist1["product_dist_socrata_main<br/><i>Publish to Socrata</i>"]:::distStyle
    dist2["product_dist_socrata_alt<br/><i>Publish to Socrata (alt)</i>"]:::distStyle
    dist3["product_dist_bytes_public<br/><i>Publish to S3/Bytes</i>"]:::distStyle

    %% Flow
    plan --> source
    source --> build
    build --> build_artifacts
    build_artifacts --> qa
    qa --> qa_artifacts
    qa_artifacts --> package
    package --> review

    %% Distribution fan-out (parallel)
    review --> dist1
    review --> dist2
    review --> dist3

    %% Notes
    note1["🔒 PRIVATE bucket:<br/>Build reports, QA reports<br/>(performance stats, internal metadata)"]
    note2["🌐 PUBLIC bucket:<br/>Dataset files, QA artifacts,<br/>packaged files<br/>(external distribution)"]

    style note1 fill:#ffcdd2,stroke:#c62828,stroke-dasharray: 5 5
    style note2 fill:#c8e6c9,stroke:#388e3c,stroke-dasharray: 5 5
```

## Simplified View (Minimal Pipeline)

For products without QA or packaging steps:

```mermaid
graph TD
    classDef planStyle fill:#e1f5ff,stroke:#0288d1,stroke-width:2px
    classDef buildStyle fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef reviewStyle fill:#ffebee,stroke:#c62828,stroke-width:3px
    classDef distStyle fill:#e0f2f1,stroke:#00796b,stroke-width:2px

    plan["product_plan"]:::planStyle
    source["product_source_data"]:::planStyle
    build["product_build"]:::buildStyle
    artifacts["product_build_artifacts"]:::buildStyle
    review["product_external_review<br/>(manual gate)"]:::reviewStyle
    dist1["product_dist_socrata"]:::distStyle
    dist2["product_dist_bytes"]:::distStyle

    plan --> source --> build --> artifacts --> review
    review --> dist1
    review --> dist2
```

## Key Characteristics

### Linear Sequential Flow
- Plan → Source Data → Build → Build Artifacts → QA → QA Artifacts → Package → External Review
- Each step depends on the previous step completing

### Fan-Out at Distribution
- **All distribution assets run in parallel** after external review approval
- Independent destinations don't block each other
- Example: Socrata publish failure doesn't prevent S3 upload

### Materialized Assets vs. Side Effects

| Asset | Materialized Artifact | Side Effects (Ephemeral) |
|-------|----------------------|--------------------------|
| `product_plan` | recipe.lock.yml in blob storage | - |
| `product_source_data` | build_metadata.json | Postgres tables in build schema |
| `product_build` | Build report (PRIVATE bucket) | Postgres tables OR local files |
| `product_build_artifacts` | Dataset files (PUBLIC bucket) | - |
| `product_qa` | QA report (PRIVATE bucket) | QA validation tables |
| `product_qa_artifacts` | QA output files (PUBLIC bucket) | - |
| `product_package_artifacts` | Enhanced files (PUBLIC bucket) | - |
| `product_external_review` | Approval metadata | - |
| `product_dist_*` | Published artifacts | - |

### Asset Grouping in Dagster UI

All assets for a product are in one group:
- `group_name="pluto"` for all PLUTO assets (plan through distribution)
- `group_name="edde"` for all EDDE assets
- `group_name="ingest"` for ingest domain (separate)

**UI Benefit:** Click "pluto" in sidebar → see entire pipeline with lineage graph

### Optional Steps

Products can skip optional steps via recipe configuration:

```yaml
stage_config:
  qa:
    enabled: false  # Skip QA entirely

  package:
    enabled: false  # Skip package step
```

If disabled, dependency chain adjusts:
- No QA: `build_artifacts` → `external_review`
- No Package: `qa_artifacts` → `external_review`
- No QA or Package: `build_artifacts` → `external_review`

## Build Types

Different products can use different build approaches:

### SQL/DBT Build (Postgres-based)
```mermaid
graph LR
    A[Source Data<br/>in Postgres] --> B[Build runs<br/>SQL/dbt]
    B --> C[Output tables<br/>in Postgres]
    C --> D[Export tables<br/>to files]
    D --> E[Upload to<br/>blob storage]

    style B fill:#fff3e0
    style C fill:#ffe0b2
    style D fill:#ffcc80
```

### Python Build (File-based)
```mermaid
graph LR
    A[Source Data<br/>in files] --> B[Build runs<br/>Python code]
    B --> C[Output files<br/>generated]
    C --> D[Upload to<br/>blob storage]

    style B fill:#fff3e0
    style C fill:#ffcc80
```

## Storage Strategy

### Private Bucket (Internal Only)
- Build reports (performance stats, logs)
- QA reports (validation details)
- Any operational/telemetry data

### Public Bucket (External Distribution)
- Exported dataset files (shapefiles, parquet, CSV)
- QA artifacts (validation outputs for review)
- Packaged files (with READMEs, metadata)

Path structure: `{product}/builds/{version}/[qaqc/]`
- Example: `pluto/builds/26v1/pluto_26v1.shp`
- Example: `pluto/builds/26v1/qaqc/validation_results.csv`

## Example: PLUTO Product

Concrete example with PLUTO:

```mermaid
graph TD
    classDef planStyle fill:#e1f5ff,stroke:#0288d1,stroke-width:2px
    classDef buildStyle fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef qaStyle fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef reviewStyle fill:#ffebee,stroke:#c62828,stroke-width:3px
    classDef distStyle fill:#e0f2f1,stroke:#00796b,stroke-width:2px

    plan["pluto_plan<br/>26v1"]:::planStyle
    source["pluto_source_data<br/>Load MapPLUTO, DTM, etc."]:::planStyle
    build["pluto_build<br/>Run SQL transformations"]:::buildStyle
    artifacts["pluto_build_artifacts<br/>Export to Shapefile, Parquet"]:::buildStyle
    qa["pluto_qa<br/>Geometry validation"]:::qaStyle
    qa_artifacts["pluto_qa_artifacts<br/>Export validation CSVs"]:::qaStyle
    review["pluto_external_review<br/>DCP approval"]:::reviewStyle

    dist1["pluto_dist_socrata_main<br/>NYC Open Data - Main"]:::distStyle
    dist2["pluto_dist_socrata_water<br/>NYC Open Data - With Water"]:::distStyle
    dist3["pluto_dist_bytes_public<br/>EDM Bytes Public"]:::distStyle

    plan --> source --> build --> artifacts --> qa --> qa_artifacts --> review
    review --> dist1
    review --> dist2
    review --> dist3
```

**Partition:** `26v1`
**Group:** `pluto` (all assets)
**Tags:** `product=pluto`, `lifecycle_stage=builds.*` or `dist.publish`

## Questions?

- **Q: What if build fails?**
  A: Downstream assets blocked. Postgres schema remains for debugging. No automatic cleanup.

- **Q: Can I retry just the export step?**
  A: Yes! `build_artifacts` can be re-materialized independently if postgres tables still exist.

- **Q: What if one distribution destination fails?**
  A: Other destinations proceed independently (fan-out pattern). Fix and retry failed destination.

- **Q: How do I run the entire pipeline?**
  A: Use per-product job (e.g., `build_pluto_job`) or materialize `product_external_review` and select "Materialize upstream".

- **Q: Can I skip QA for a quick test build?**
  A: Set `stage_config.qa.enabled: false` in recipe or manually materialize `external_review` after `build_artifacts`.
