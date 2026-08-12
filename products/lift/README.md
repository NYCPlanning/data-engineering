# DCAS LIFT

Product supporting DCAS's Land Inventory / Fast Track (LIFT) process.

## Important files

[recipe](https://github.com/NYCPlanning/data-engineering/blob/main/products/lift/recipe.yml)

## `lift_supplemented`

`models/product/lift_supplemented.sql` is a drop-in copy of the `lift_csv` extract (same grain,
one row per `bbl`, every other column untouched), with three empty placeholder columns
populated - `DISPLACEMENT_RISK_FORMULA`, `CPSPENTTOTAL`, `CPPROJECTS` - plus one new column,
`cp_project_ids` (a JSON array of the intersecting CPDB project ids, for QA/traceability behind
`cpspenttotal`/`cpprojects`). BBLs with no intersecting capital project get `0`/`[]`, not `null`,
for `cpspenttotal`/`cpprojects`/`cp_project_ids`.

Join path, all verified against a real build rather than assumed (see `models/intermediate/`):

- **DRI Tier**: `lift_csv.bbl` -> `pluto.bbl` (99.8% match) -> `pluto.bct2020 = ct2020.boroct2020`
  (99.997% match) -> `ct2020.nta2020 = dri.NTACode`. `dcp_ct2020` already carries `nta2020`
  directly, so it doubles as the census-tract-to-NTA crosswalk - no separate lookup dataset
  needed. `dri_tier` is `dri_subindices_indicators.DisplacementRiskIndex` (5-level categorical:
  Lowest/Low/Intermediate/High/Highest). DRI is null for ~2.6% of BBLs - confirmed this is
  complete and legitimate, not a join gap: DRI only covers 197 of NTA2020's 262 NTAs, and every
  one of the 65 missing NTAs is a park/cemetery/airport/institutional area (Central Park, Rikers
  Island, JFK, etc.) with no residential population to measure displacement risk for.
- **CPSpentTotal / CPProjects / cp_project_ids**: spatial join, `pluto.geom` `ST_Intersects`
  CPDB project geometries, grouped by `bbl` (`COUNT(DISTINCT project_id)`, `SUM(spent_total)`,
  sorted JSON list of `project_id`). Both CPDB layers are joined (`stg__cpdb_points` UNION ALL
  `stg__cpdb_poly` in `int__lift_cpdb.sql`) - confirmed via `cpdb`'s own pipeline
  (`cpdb_projects_pts`/`cpdb_projects_poly` are `cpdb_projects_shp` filtered by
  `ST_GeometryType`) that every project has exactly one geometry, point or polygon, so the two
  layers are a strict partition of one canonical project table with zero overlapping
  `project_id`s - `UNION ALL` can't double-count a project. Points-only matched 1,327 distinct
  LIFT BBLs; adding poly brings that to 5,367 - poly isn't redundant with points, most non-point
  projects (park/street/facility footprints) simply never got a point representation at all.
  Worth knowing: a handful of large public-land BBLs (Rikers Island, Flushing Meadows, Central
  Park, etc. - PLUTO represents each as one enormous lot) will show very high `cpprojects`/
  `cpspenttotal`, since every CPDB project anywhere within that huge polygon attributes entirely
  to that one BBL. Not a bug, but worth knowing before treating these numbers as "investment at
  this specific site" for LIFT's actual redevelopment-site use case.

### Initial results

From a full build against the current recipe (14,244 LIFT rows):

- **5,367** BBLs (37.7%) have at least one intersecting CPDB capital project (points + poly
  combined; 1,327 with points alone).
- **10,726** total project-lot hits, **~$1.03B** total `cpspenttotal` represented across all BBLs.
- **DRI Tier** populated for **97.4%** of BBLs; the remaining 2.6% are legitimately outside DRI's
  residential-NTA coverage (see above), not a join failure.

**Implementation note**: DuckDB's spatial extension is strict about CRS equality - `pluto.geom` is
stored labeled `OGC:CRS84` and `cpdb_projects_points.geom` as `EPSG:4326`; calling `ST_Intersects`
across them errors unless the CRSes match. Both are numerically lon/lat WGS84 in this data, so
`stg__pluto.sql` relabels with `ST_SetCRS(geom, 'EPSG:4326')` rather than actually reprojecting -
`ST_Transform` attempts to download PROJ grid files over the network on first use, which isn't
something we want as a build-time dependency.

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
