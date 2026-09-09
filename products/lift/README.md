# LIFT

Land Inventory Fast Track (LIFT) is a multi-agency effort, established by Executive Order 4, to
identify City-owned land suitable for housing. This product produces the supplemented LIFT
dataset: DCAS sends an extract of city-owned tax lots and we add DCP data on displacement risk,
capital project spending, census tract and NTA, and transit zone.

## Important files

[recipe](./recipe.yml)

[data_dictionary.csv](./data_dictionary.csv) - DCAS's field spec, generated from their workbook
by [scripts/update_data_dictionary.py](./scripts/update_data_dictionary.py). Rerun that script
when DCAS sends a new revision.

## Links

[LIFT Tracker](https://storymaps.arcgis.com/stories/afce9e93d79847608fc846878e17db8e)

[LIFT Tracker announcement](https://www.nyc.gov/mayors-office/news/2026/07/mayor-mamdani-launches-lift-tracker-to-build-affordable-housing-)

## Source data

| Dataset | Store | What it provides |
|---|---|---|
| `dcas_lift` | `edm-private` | The LIFT extract itself, one row per BBL |
| `dcp_mappluto_wi` | `edm-recipes` | Lot geometry, census tract (`bct2020`), transit zone |
| `dcp_ct2020` | `edm-recipes` | Census tract to NTA crosswalk |
| `dcp_dri_subindices_indicators` | `edm-private` | Displacement Risk Index, by NTA |
| `db-cpdb` (points and poly) | `edm-publishing` | Capital project geometries and spending |

DCAS assembles the extract on their side from several IPIS tables plus a block of PLUTO fields
and a block of LIFT fields. The `ZZZ_*_ZZZ` columns in the CSV mark those section boundaries,
and each section carries its own join key (`PLUTO_BBL`, `HOLDS_BBL`, `LEASE_OUT_BBL`, `USE_BBL`,
`ULURP_BBL`). The upstream systems are DCAS's IPIS, DCP datasets (PLUTO, CPDB, DRI), and a LIFT
tracker maintained by City Hall. Sections that are one-to-many against a BBL arrive as plural
columns (`HOLDERS`, `TENANTS`, `LEASE_TYPES`, and so on) so the extract holds one row per BBL.

LIFT releases quarterly, timed to coincide with PLUTO major releases.

## What we add

`models/product/lift_supplemented.sql` copies `lift_csv` at the same grain, one row per `bbl`
with every other column untouched. It fills the three columns the source extract leaves empty
and adds six more.

Filled in:

| Column | Value |
|---|---|
| `displacement_risk_formula` | DRI tier for the BBL's NTA |
| `cpspenttotal` | sum of `spent_total` across intersecting CPDB projects |
| `cpprojects` | count of distinct intersecting CPDB projects |

Added:

| Column | Value |
|---|---|
| `boroct2020`, `nta2020`, `ntaname` | census tract and NTA the BBL resolves to |
| `cp_project_ids`, `cp_project_descriptions` | comma- and pipe-delimited lists of the intersecting projects, for tracing `cpspenttotal` and `cpprojects` |
| `trnstzone` | PLUTO's transit zone designation, carried straight through |

BBLs with no intersecting capital project get `0` and `''`, not null.

### Join paths

**Displacement risk** (`models/intermediate/int__lift_dri.sql`): DRI is published at the NTA
level and PLUTO doesn't carry NTA, so the route is `lift_csv.bbl` -> `pluto.bbl` ->
`pluto.bct2020 = ct2020.boroct2020` -> `ct2020.nta2020 = dri.NTACode`. `dcp_ct2020` carries
`nta2020` directly, so it doubles as the tract-to-NTA crosswalk and no separate lookup dataset
is needed. `dri_tier` is `dri_subindices_indicators.DisplacementRiskIndex`, a five-level
categorical. The census tract and NTA columns fall
out of the same join, and the DRI join stays a LEFT join so a BBL without a DRI value keeps its
tract and NTA.

**Capital projects** (`models/intermediate/int__lift_cpdb.sql`): CPDB has no BBL field, so this
is a spatial join. CPDB project geometries `ST_Intersects` `pluto.geom`, grouped by `bbl`. Both
CPDB layers are unioned; that model's header comment explains why points and polygons can't
double-count a project.

## Limitations

**Large public-land lots inflate `cpspenttotal` and `cpprojects`.** PLUTO represents Rikers
Island, Flushing Meadows, Central Park and similar sites as one enormous lot, so every capital
project anywhere inside that polygon attributes to that single BBL. The numbers are right for
the lot, but they aren't "investment at this site" the way LIFT otherwise reads them.

**DRI doesn't cover every NTA.** The Displacement Risk Index measures residential displacement,
so it has no value for park, cemetery, airport, and institutional NTAs. `displacement_risk_formula`
is null for BBLs in those areas. That's a coverage gap in the source rather than a join failure,
and `boroct2020`, `nta2020` and `ntaname` are still populated for those BBLs.

**`data_dictionary.csv` describes a target, not the current output.** Several fields in it have
no source yet, and its field names don't all match the columns in the extract DCAS currently
sends.

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
export only covers `csv`, `dat` and `parquet` so far. `shp` and `gdb` export from DuckDB raises
`NotImplementedError`; those still require a postgres-backed recipe.

## Dagster

`apps/dagster/builds/assets.py` discovers every product under `products/` with a `recipe.yml`
and generates `plan_lift` / `build_lift` / `draft_lift` assets from it, so there's no
LIFT-specific Dagster code. `build_lift` runs each command in `recipe.yml`'s
`stage_config.builds.build.commands` in order, each as its own subprocess.

Those subprocesses share no state, so `dbt_build` recomputes `DUCKDB_PATH` from
`$BUILD_ENV_OUTPUT_DIR` and `$VERSION` every time rather than relying on an earlier step's
`export`. Both are set before any command runs.
