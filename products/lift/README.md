# DCAS LIFT

Product supporting DCAS's Land Inventory / Fast Track (LIFT) process.

## Important files

[recipe](https://github.com/NYCPlanning/data-engineering/blob/main/products/lift/recipe.yml)

## Running locally

This product loads its recipe sources directly into **DuckDB**, not Postgres - `BUILD_ENGINE_*`
env vars aren't needed. `BUILD_ENGINE_SCHEMA` still matters, though: root `.envrc` sets it from
your git branch, and both the loader and dbt use it as the schema name.

### Plan/Load

Compile the `recipe` (into `recipe.lock.yml`), then load source data into a `.duckdb` file:

```bash
python3 -m dcpy.lifecycle.builds.plan recipe
dcpy lifecycle builds load load --recipe-path recipe.lock.yml
```

### Setup dbt

dbt needs to know which `.duckdb` file to connect to - point `DUCKDB_PATH` at the file the
loader just created (the same lookup `dcp_duck_my_build` uses):

```bash
export DUCKDB_PATH=$(python3 -m dcpy lifecycle builds build path --duckdb --recipe recipe)
dbt deps
dbt debug
```

### Build

```bash
dbt test --select "source:*"
dbt build --select staging
dbt build --select intermediate
dbt build --select product
```

Or run the whole thing (load excluded) via:

```bash
./bash/build.sh
```

`dbt-duckdb` isn't in the compiled `requirements.txt` yet (only added to
`admin/run_environment/requirements.in` so far) - install it into your venv directly
(`uv pip install dbt-duckdb --no-deps`) until the pin is compiled in.

### Export

`recipe.yml` declares one export - `lift_supplemented` as a CSV (see `exports:` at the bottom of
the recipe). Run it after the dbt build:

```bash
python3 -m dcpy lifecycle builds export export --recipe-path recipe.lock.yml
```

This writes `output/dataset_files/lift_supplemented.csv` (plus `output/output.zip`). DuckDB
export support (`export_dataset_from_duckdb` / `DuckDBClient.export_to_csv` /
`export_to_parquet` in `dcpy/utils/duckdb.py`) is new - it only covers `csv`/`dat`/`parquet`
formats so far. `shp`/`gdb` export from DuckDB isn't implemented (raises `NotImplementedError`);
those still require a postgres-backed recipe, same as before.

## Dagster

`recipe.yml`'s `stage_config.builds.build.commands` (mirroring `products/edde/recipe.yml`) is
what wires this product into Dagster - `apps/dagster/builds/assets.py` auto-discovers every
product under `products/` with a `recipe.yml` (`dcpy.lifecycle.list_products()`) and generates
`plan_lift`/`build_lift`/`draft_lift`/etc. assets generically from it, so no lift- or
dagster-specific code was needed here. `build_lift` dynamically maps one op per command in
`commands`, run in order (`load_recipe_data`, then `dbt_deps`, then `dbt_build`, then
`export_recipe_data` - the first/last are Dagster's built-in special-cased names, not listed in
the recipe).

Unlike `edde`'s `command_type: python` steps, lift's are `command_type: shell` (`dbt deps`,
`dbt build ...`) since there's no `build.py` module here - just dbt. Each command runs as its
own subprocess with no shared state, so `dbt_build` computes `DUCKDB_PATH` fresh every time from
`$BUILD_ENV_OUTPUT_DIR`/`$VERSION` rather than relying on an earlier step's `export`. Both are
guaranteed present in the environment before any command runs - `VERSION` because
`recipe.env["VERSION"] = recipe.version` is set unconditionally at plan time
(`dcpy/lifecycle/builds/plan.py`), and `BUILD_ENV_OUTPUT_DIR` because Dagster's `_execute_command`
op (and `run_single_command`'s `build_directory` param generally) always sets it from the build
directory before executing a command.

Verified by calling `dcpy.lifecycle.builds.build.run_single_command()` directly for each command
name, exactly as `apps/dagster/builds/assets.py`'s `_execute_command` op does - not just by
reading the code.

## `lift_supplemented`

The DCAS customer asked for three fields added to the `lift_csv` extract - DRI Tier,
CPSpentTotal (total capital-project spend intersecting the lot), and CPProjects (count of
capital projects intersecting the lot). The source spreadsheet already ships empty placeholder
columns for exactly these - `DISPLACEMENT_RISK_FORMULA`, `CPSPENTTOTAL`, `CPPROJECTS` - so
`models/product/lift_supplemented.sql` is a drop-in copy of `lift_csv` (same grain, one row per
`bbl`, every other column untouched) with those three populated. BBLs with no intersecting
capital project get `0`, not `null`, for `cpspenttotal`/`cpprojects`.

Join path, all verified against a real build rather than assumed (see `models/intermediate/`):

- **DRI Tier**: `lift_csv.bbl` -> `pluto.bbl` (99.8% match) -> `pluto.bct2020 = ct2020.boroct2020`
  (99.997% match) -> `ct2020.nta2020 = dri.NTACode`. `dcp_ct2020` already carries `nta2020`
  directly, so it doubles as the census-tract-to-NTA crosswalk mentioned in the request - no
  separate lookup dataset needed. `dri_tier` is `dri_subindices_indicators.DisplacementRiskIndex`
  (5-level categorical: Lowest/Low/Intermediate/High/Highest).
- **CPSpentTotal / CPProjects**: spatial join, `pluto.geom` `ST_Intersects` `cpdb_projects_points.geom`,
  grouped by `bbl` (`COUNT(DISTINCT project_id)`, `SUM(spent_total)`). Only the CPDB *points* layer
  is joined so far, per the initial request - `cpdb_projects_poly` is loaded and listed as a source
  but not yet used.

**Open question worth a sanity check with capital planning**: CPDB has several `*total` dollar
columns (`pctotal`/`adtotal`/`altotal`/`cototal`/`sptotal`/`sptotalcb`), following what looks like
a Prior-Commitments / Adopted-Budget / All-Commitments / Current-Commitments / Spending-Plan
naming scheme. `product-metadata/products/cpdb/projects/metadata.yml` documents `sptotal` (id
`spent_total`) as *"Sum of the total funding spent associated with the project within the City's
budget"* - which is what `stg__cpdb_points.spent_total` uses - as opposed to `sptotalcb` (id
`spent_total_checkbooknyc`, *"Sum of check values from Checkbook NYC"*), a separate,
alternatively-sourced total. `sptotal` reads as the right field for "total spend," but worth
confirming with capital planning that it's the one they meant, given how many adjacent `*total`
columns exist.

**Implementation note**: DuckDB's spatial extension is strict about CRS equality - `pluto.geom` is
stored labeled `OGC:CRS84` and `cpdb_projects_points.geom` as `EPSG:4326`; calling `ST_Intersects`
across them errors unless the CRSes match. Both are numerically lon/lat WGS84 in this data, so
`stg__pluto.sql` relabels with `ST_SetCRS(geom, 'EPSG:4326')` rather than actually reprojecting -
`ST_Transform` attempts to download PROJ grid files over the network on first use, which isn't
something we want as a build-time dependency.
