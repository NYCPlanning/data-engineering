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
| `hpd_limited_affordability_areas` | `edm-private` | Limited Affordability Area boundaries |
| `hpd_rezoning_tracker` | `edm-recipes` (ingested from NYC Open Data) | Rezoning-commitment tracker, by neighborhood study area |
| `dcp_nta2020` | `edm-recipes` | NTA names and boundaries, for the `seeds/rezoning_areas.csv` crosswalk |

DCAS assembles the extract on their side from several IPIS tables plus a block of PLUTO fields
and a block of LIFT fields. The `ZZZ_*_ZZZ` columns in the CSV mark those section boundaries,
and each section carries its own join key (`PLUTO_BBL`, `HOLDS_BBL`, `LEASE_OUT_BBL`, `USE_BBL`,
`ULURP_BBL`). The upstream systems are DCAS's IPIS, DCP datasets (PLUTO, CPDB, DRI), and a LIFT
tracker maintained by City Hall. Sections that are one-to-many against a BBL arrive as plural
columns (`HOLDERS`, `TENANTS`, `LEASE_TYPES`, and so on) so the extract holds one row per BBL.

LIFT releases quarterly, timed to coincide with PLUTO major releases.

## What we add

`models/product/lift_supplemented.sql` copies `lift_csv` at the same grain, one row per `bbl`
with every other column untouched. It fills the six columns the source extract leaves empty
and adds six more.

Filled in:

| Column | Value |
|---|---|
| `displacement_risk_formula` | DRI tier for the BBL's NTA |
| `cpspenttotal` | sum of `spent_total` across intersecting CPDB projects |
| `cpprojects` | count of distinct intersecting CPDB projects |
| `laa` | `'Y'` if the BBL's lot intersects a Limited Affordability Area, else `NULL` |
| `poa` | count + status breakdown of rezoning-tracker commitments tied to the BBL's NTA, e.g. `"63 commitments (49 done, 13 in progress, 1 other)"`, or `NULL` |
| `poa_commitment` | `"id: title (stage)"` for each of those commitments, pipe-delimited, or `NULL` |

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

**Limited Affordability Areas** (`models/intermediate/int__lift_laa.sql`): same pattern as
CPDB - no BBL field on the source, so `ST_Intersects` against `pluto.geom` decides membership.
Unlike CPDB there's no attribute data to aggregate (see Limitations), so the intermediate model
is just the distinct set of intersecting BBLs; `lift_supplemented` turns presence/absence into
`'Y'`/`NULL`.

**Rezoning commitments** (`models/intermediate/int__lift_rezoning_tracker.sql`): the NYC
Rezoning Tracker (`hpd_rezoning_tracker`) has no BBL *or* geometry - it's keyed by
`rezoning_area`, one of 13 named neighborhood-scale study areas (East Harlem, Gowanus, Inwood,
etc.), crossed with year and commitment. `seeds/rezoning_areas.csv` is a hand-built crosswalk
from `rezoning_area` to the NTA(s) it covers, checked by textual overlap against `dcp_nta2020`
(e.g. `Gowanus` -> `Carroll Gardens-Cobble Hill-Gowanus-Red Hook`; some areas, like `Bay Street`,
span more than one NTA). Areas with no clear NTA match (`Atlantic Avenue`, `Bronx Metro-North`,
`Jerome`, and the two citywide rollups) are left in the seed with a blank `nta_name` and dropped
before joining. A BBL's NTA (same `pluto.boroct2020 -> ct2020.boroct2020` path as
`int__lift_dri`) then decides which area's commitments apply to it. The tracker is a yearly
progress snapshot - the same commitment recurs across multiple years with an evolving
`commitment_stage` - so the intermediate model first collapses to each commitment's latest year
before counting. `stg__hpd_rezoning_tracker` assigns an artificial `commitment_id` (source has no
usable one - see Limitations) that `poa_commitment` uses for traceability.

## Limitations

**`hpd_limited_affordability_areas` has no attribute data, only geometry.** The shapefile in
`edm-private` was uploaded as a bare `.shp`, without its `.shx`/`.dbf`/`.prj` sidecars. DuckDB's
spatial loader now passes `SHAPE_RESTORE_SHX=YES` (see `dcpy/utils/duckdb.py`), which lets GDAL
regenerate a missing `.shx` index from the `.shp` itself - but a `.dbf` can't be reconstructed
the same way, since it holds the actual attribute rows, not a derivable index. That's why `laa`
can only be a presence flag: there's no area name/id in the source to carry through for
traceability the way `cp_project_ids`/`cp_project_descriptions` do for CPDB. The CRS is likewise
undeclared (no `.prj`); `stg__hpd_limited_affordability_areas` relabels it to EPSG:4326 based on
the raw coordinate range, same non-`ST_Transform` approach as `stg__pluto`. If a fuller extract
(with sidecars) gets uploaded later, re-running the loader will pick up the real CRS and any
attribute columns without further code changes.

**`poa`/`poa_commitment` are matched at the NTA level, not the individual site level.** A BBL
gets *every* commitment tied to its NTA's rezoning study area, not commitments specific to that
lot - the rezoning tracker has no finer-grained location data than the study area itself. Read
these as "rezoning activity in this BBL's neighborhood," not "commitments about this BBL."

**The rezoning tracker has no reliable id, so `commitment_id` is artificial.** The closest thing
to a source id, `map_order`, isn't stable: in 25 cases one `map_order` covers multiple different
`commitment_title`s within the same area, and titles drift across multiple `map_order` values
across years. `stg__hpd_rezoning_tracker` assigns `commitment_id` via
`DENSE_RANK() OVER (ORDER BY rezoning_area, commitment_title)` instead - stable within a build as
long as the distinct (area, title) set doesn't change, but it's ours, not the source's, and isn't
guaranteed stable across a source refresh that adds or removes commitments.

**`seeds/rezoning_areas.csv` is a manually-curated crosswalk, not authoritative.** Matches were
made by checking textual overlap between `rezoning_area` and `dcp_nta2020.ntaname` (e.g. `Bay
Street` -> `St. George-New Brighton` and `Tompkinsville-Stapleton-Clifton-Fox Hills`, since Bay
Street runs through both). Areas with no textual anchor were left unmatched rather than guessed.
If DCP publishes an authoritative rezoning-study-area boundary layer, spatially joining against
that (like LAA/CPDB) would replace this and pick up the currently-unmatched areas too.

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

A reused build directory (same product/version) only drops+recreates the individual source
tables it's re-inserting, not the whole schema - so dbt-built objects (seeds, models) from a
prior build stick around. Add `--clear-duckdb-schema` for a true clean slate.

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
dbt seed
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
(`uv pip install dbt-duckdb`) until the pin is compiled in. Don't use `--no-deps`: `dbt-core`
pulls in `dbt-adapters` and friends, and dbt fails at import (`No module named
'dbt.adapters.factory'`) without them.

### Export

`recipe.yml` declares two exports (see `exports:` at the bottom of the recipe) - `lift_supplemented`
and `rezoning_commitments` (the full NYC Rezoning Tracker commitment listing, one row per
`commitment_id`, for tracing the ids embedded in `lift_supplemented.poa_commitment` back to full
detail). Both are CSVs today; likely candidates for bundling into a single XLSX (one sheet each)
at some point. Run the export after the dbt build:

```bash
python3 -m dcpy lifecycle builds export export --recipe-path recipe.lock.yml
```

This writes `output/dataset_files/lift_supplemented.csv` and
`output/dataset_files/rezoning_commitments.csv` (plus `output/output.zip`). DuckDB
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
