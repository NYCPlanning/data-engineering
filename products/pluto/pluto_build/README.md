# PLUTO Build Process

## Build Sequence

The PLUTO build follows this sequence:

1. **00_setup.sh** - Drops existing tables to start fresh
2. **01a_dbt_staging.sh** - 🆕 Loads DBT seeds & materializes staging models
3. **02_build.sh** - Runs legacy SQL to build PLUTO
4. **03_corrections.sh** - Applies corrections
5. **04_archive.sh** - Archives the build
6. **05_qaqc.sh** - Runs QAQC checks
7. **06_export.sh** - Exports final output
8. **07_custom_qaqc.sh** - Custom QAQC

## Important: DBT Seeds & Staging Models

As of Phase 1 of the DBT migration, **01a_dbt_staging.sh must run before 02_build.sh**.

### What 01a_dbt_staging.sh does:

1. **Loads DBT seeds** (`dbt seed`)
   - Loads 9 CSV lookup tables from `seeds/` directory
   - Creates tables: pluto_input_research, dcp_zoning_maxfar, lookup_bldgclass, etc.
   - Replaces old CSV loading via \COPY commands

2. **Materializes DBT staging models** (`dbt run --select staging`)
   - Creates 40 staging models from raw recipe data
   - Creates tables like `stg__dcp_councildistricts_wi`, `stg__lpc_landmarks`, etc.
   - These tables are then used by legacy SQL in 02_build.sh

### Dependencies:
- Requires `dbt` to be installed
- Requires recipe data to be loaded first (via recipe.yml)
- Uses DBT_TARGET environment variable (defaults to 'dev')

## Running the Build

```bash
# Full build sequence (after recipe loads data):
./00_setup.sh
./01a_dbt_staging.sh  # 🆕 DBT seeds + staging models
./02_build.sh
./03_corrections.sh
# ... continue with remaining steps
```

## Lookup Tables (Seeds)

The following lookup/reference tables are now managed as DBT seeds in `seeds/`:
- `pluto_input_research.csv` - Manual corrections (27k rows)
- `dcp_zoning_maxfar.csv` - Max FAR by zoning district
- `lookup_bldgclass.csv` - Building class lookup
- `lookup_lottype.csv` - Lot type lookup
- `pluto_input_bsmtcode.csv` - Basement codes
- `pluto_input_condo_bldgclass.csv` - Condo building classes
- `pluto_input_condolot_descriptiveattributes.csv` - Condo attributes (8k rows)
- `pluto_input_landuse_bldgclass.csv` - Land use mappings
- `zoning_district_class_descriptions.csv` - Zoning descriptions

These are loaded automatically by `dbt seed` in 01a_dbt_staging.sh.

## Migration Status

**Phase 1 (Complete):** Staging layer
- ✅ 40 staging models created
- ✅ All legacy SQL refactored to use staging models
- ✅ preprocessing.sql eliminated

**Phase 2-5 (Future):** Migrate remaining SQL to DBT intermediate/product models
