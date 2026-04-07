# DBT REFACTOR

## Overview
Migrate PLUTO SQL build files to dbt intermediate models for targeted re-running. This enables rebuilding specific calculations without full pipeline runs.

**CRITICAL: Preserve existing logic exactly.** If you find bugs, document them but DO NOT fix during migration. Output must remain identical.

## End Goals
- All SQL transformations are declarative (SELECT, not UPDATE/INSERT)
- Aligns with docs/dbt/project_conventions.md
- No potentially circular logic
  - e.g. tables that feed into pluto should not read from or modify pluto. Currently they do, which is fine for the moment, but should be refactored away. 
- One stg__* table per recipe source
- Business logic in intermediate models joined in product layer

## Major Work Items

### 1. DBT'ify PLUTO Spine (Foundation)
**Goal:** Convert pluto table creation from SQL to dbt product model

**Current state:**
- `sql/create.sql` creates empty pluto table (113 columns)
- `sql/allocated.sql` and `sql/geocodes.sql` populate initial data via UPDATE
- Result: ~1M row base table that other models UPDATE

**Target state:**
- `models/product/pluto.sql` - declarative SELECT assembling all BBL records with base attributes
- Must support dev mode: sample to ~1k BBLs for fast iteration
- Materialized as table with BBL unique constraint

**Impact:** Enables dev mode for entire pipeline (~1.5hr → ~1min builds)

### 2. DBT'ify Corrections Script
**Goal:** Convert `03_corrections.sh` from UPDATE statements to dbt model

**Current problem:** 
- After `02_build.sh` completes, corrections directly UPDATE pluto table
- Makes re-running stage 2 (dbt enrichments) impossible without full rebuild
- ~12 correction files modify pluto in-place

**Target state:**
- `models/product/pluto_corrections.sql` or similar
- Corrections applied via LEFT JOIN in final assembly, not UPDATE
- Enables iterative development on enrichment logic

### 3. Consolidate Export Logic with DBT Macro
**Goal:** Reduce duplication in export scripts

**Current state:**
- `06_export.sh` has repetitive export patterns:
  - 2x `fgdb_export_pluto` calls (mappluto_gdb, mappluto_unclipped_gdb)
  - 2x `shp_export_pluto` calls (mappluto, mappluto_unclipped)
  - Multiple CSV exports with similar patterns

**Target state:**
- Create dbt macro to generate export SQL for different formats
- Reduce boilerplate and ensure consistency




## Intermediate Model Structure

### File Location
- Simple transformations → `models/intermediate/simple/int_pluto__{topic}.sql`
- Complex multi-file logic → `models/intermediate/{topic}/int_{topic}__{name}.sql`

### Required Template
```sql
-- Migrated from: pluto_build/sql/{original}.sql

{{ config(
    materialized='table',
    indexes=[{'columns': ['bbl'], 'unique': True}]  -- See README for BBL indexing requirements
) }}

{% set dev_mode = env_var('PLUTO_DEV_MODE', 'false') == 'true' %}

WITH base AS (
    SELECT * FROM {{ ref('stg__pluto_input_geocodes') }}
    {% if dev_mode %}
    WHERE bbl IN (SELECT bbl FROM {{ ref('dev_sample_bbls') }})
    {% endif %}
),

calculations AS (
    -- Your logic here
    SELECT 
        bbl,
        field1,
        field2
    FROM base
)

SELECT * FROM calculations
```

Add schema tests (see README for details): unique and not_null on bbl column.

## Critical Migration Rules

### 1. Preserve Data Types
**Check downstream SQL before changing types.** Wave 0 lesson: xcoord/ycoord stored as text despite containing integers because downstream SQL used `ltrim()` which requires text input.

### 2. Multi-Table Updates
If legacy SQL updates multiple tables, split logic across ALL affected dbt models. Example: `primebbl.sql` updated both `pluto_rpad_geo` AND `dof_pts_propmaster`.

### 3. Staging Table Mutations
Legacy SQL often ALTERs or UPDATEs staging tables. In dbt: read staging as-is, compute in CTEs, never mutate sources.

## Integration Pattern

Product layer joins all intermediate models:
```sql
-- models/product/pluto_enriched.sql
{{ config(
    materialized='table',
    indexes=[{'columns': ['bbl'], 'unique': True}]
) }}

SELECT 
    base.*,
    far.builtfar,
    far.residfar,
    lpc.landmark,
    lpc.landmarked
FROM {{ ref('stg__pluto_base') }} base
LEFT JOIN {{ ref('int_pluto__far') }} far USING (bbl)
LEFT JOIN {{ ref('int_pluto__lpc') }} lpc USING (bbl)
```

Legacy SQL applies: `UPDATE pluto SET (fields...) = (pe.fields...) FROM pluto_enriched pe WHERE pluto.bbl = pe.bbl`

## Testing Workflow

**Note:** Dev mode requires completing Major Work Item #1 (DBT'ify PLUTO Spine) first.

```bash
# 1. Build new models (dev mode - fast)
cd products/pluto
export PLUTO_DEV_MODE=true
eval "$(direnv export bash)" && dbt run --select int_pluto__example
# Should complete in seconds with ~1k BBL sample

# 2. Lint
cd /Users/alexrichey/dev/data-engineering-de2  # repo root
sqlfluff fix --dialect postgres --templater jinja products/pluto/models/intermediate/simple/int_pluto__example.sql

# 3. Test full build (optional, slow - only when needed)
cd products/pluto
unset PLUTO_DEV_MODE
eval "$(direnv export bash)" && dbt run --select int_pluto__example
# Full 1M BBL run takes significantly longer
```

## File Cleanup

After integration:
1. Remove `run_sql_file` call from `pluto_build/02_build.sh`
3. Update any file references in other SQL files

## Migration Checklist

Before closing issue:
- [ ] Model outputs bbl + calculated fields only
- [ ] Dev mode sampling implemented
- [ ] BBL index and schema tests configured (see README)
- [ ] Migrated comment at top of file
- [ ] sqlfluff passes
- [ ] 02_build.sh updated
- [ ] Downstream references updated

## Environment Setup

**Local development only:** Use direnv to load environment variables:
```bash
source load_direnv.sh && <command>
# OR
eval "$(direnv export bash)" && <command>
```

**Do NOT include direnv in project scripts.** Scripts should receive environment variables from their caller.


## DBT Model Conventions

### BBL Indexing
All intermediate and product models that join on BBL require a unique BBL index for performance:
```sql
{{ config(
    materialized='table',
    indexes=[{'columns': ['bbl'], 'unique': True}]
) }}
```

### BBL Schema Tests
All intermediate models must include BBL uniqueness and null checks in `models/intermediate/{folder}/schema.yml`:
```yaml
models:
  - name: int_pluto__example
    columns:
      - name: bbl
        tests:
          - unique
          - not_null
```
