# PLUTO Build Process

## Build Sequence

The PLUTO build follows this sequence:

1. **00_setup.sh** - Drops existing tables to start fresh
2. **01_load_local_csvs.sh** - Loads local CSV data
3. **01a_dbt_staging.sh** - 🆕 Materializes DBT staging models
4. **02_build.sh** - Runs legacy SQL to build PLUTO
5. **03_corrections.sh** - Applies corrections
6. **04_archive.sh** - Archives the build
7. **05_qaqc.sh** - Runs QAQC checks
8. **06_export.sh** - Exports final output
9. **07_custom_qaqc.sh** - Custom QAQC

## Important: DBT Staging Models

As of Phase 1 of the DBT migration, **01a_dbt_staging.sh must run before 02_build.sh**.

The legacy SQL files in `sql/` now reference DBT staging models (prefixed with `stg__`) instead of raw source tables. These staging models must be materialized first.

### What 01a_dbt_staging.sh does:
- Runs `dbt run --select staging`
- Materializes 40 staging models from raw recipe data
- Creates tables like `stg__dcp_councildistricts_wi`, `stg__lpc_landmarks`, etc.
- These tables are then used by legacy SQL in 02_build.sh

### Dependencies:
- Requires `dbt` to be installed
- Requires recipe data to be loaded first (via recipe.yml)
- Uses BUILD_ENGINE_SCHEMA environment variable

## Running the Build

```bash
# Full build sequence (after recipe loads data):
./00_setup.sh
./01_load_local_csvs.sh
./01a_dbt_staging.sh  # 🆕 DBT staging models
./02_build.sh
./03_corrections.sh
# ... continue with remaining steps
```

## Migration Status

**Phase 1 (Complete):** Staging layer
- ✅ 40 staging models created
- ✅ All legacy SQL refactored to use staging models
- ✅ preprocessing.sql eliminated

**Phase 2-5 (Future):** Migrate remaining SQL to DBT intermediate/product models
