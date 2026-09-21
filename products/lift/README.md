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
| `dcp_zoningmapamendments` | `edm-recipes` | Mapped geometry for each adopted rezoning, by ULURP number - joined via `seeds/rezoning_areas.csv` |
| `dcp_nta2020` | `edm-recipes` | NTA names and boundaries; no longer used in any model (see Limitations) |
| `cityhall_public_sites_for_housing` | `edm-private` | City Hall's "Public Sites for Housing" tracker - redevelopment strategy/priority/cost fields, by BBL |

DCAS assembles the extract on their side from several IPIS tables plus a block of PLUTO fields
and a block of LIFT fields. The `ZZZ_*_ZZZ` columns in the CSV mark those section boundaries,
and each section carries its own join key (`PLUTO_BBL`, `HOLDS_BBL`, `LEASE_OUT_BBL`, `USE_BBL`,
`ULURP_BBL`). The upstream systems are DCAS's IPIS, DCP datasets (PLUTO, CPDB, DRI), and a LIFT
tracker maintained by City Hall. Sections that are one-to-many against a BBL arrive as plural
columns (`HOLDERS`, `TENANTS`, `LEASE_TYPES`, and so on) so the extract holds one row per BBL.

LIFT releases quarterly, timed to coincide with PLUTO major releases.

## What we add

`models/product/lift_supplemented.sql` copies `lift_csv` at the same grain, one row per `bbl`
with every other column untouched. It fills the placeholder columns the source extract leaves
empty and adds six more.

Filled in:

| Column | Value |
|---|---|
| `displacement_risk_formula` | DRI tier for the BBL's NTA |
| `cpspenttotal` | sum of `spent_total` across intersecting CPDB projects |
| `cpprojects` | count of distinct intersecting CPDB projects |
| `laa` | `'Y'` if the BBL's lot intersects a Limited Affordability Area, else `NULL` |
| `poa` | count + status breakdown of rezoning-tracker commitments for the rezoning area whose mapped ULURP geometry intersects the BBL's lot, e.g. `"63 commitments (49 done, 13 in progress, 1 other)"`, or `NULL` |
| `poa_commitment` | `"id: title (stage)"` for each of those commitments, pipe-delimited, or `NULL` |
| `zzz_lift_data_zzz` | Public Sites `Site` name, by BBL - see Limitations, this is an unusual mapping |
| `unitpotential` | Public Sites `UnitPot`, by BBL |
| `dev_flags` | Public Sites `Key Challenges`, by BBL |
| `redev_summary` | Public Sites `Summary`, by BBL |
| `redev_strategy` | Public Sites `Redevelopment Strategy`, by BBL |
| `redev_priority` | Public Sites `Prioritization`, by BBL - see Limitations, source field named in the mapping doesn't exist verbatim |
| `redev_study` | Public Sites `Study Summary`, by BBL |
| `cp_remediation` | Public Sites `OMB Remediation Cost`, by BBL |
| `siteprep_costs` | Public Sites `Site Preparation Costs`, by BBL |
| `siteprep_desc` | Public Sites `Site Preparation Description`, by BBL |
| `rlv_amount` | Public Sites `RLV`, by BBL |
| `rlv_desc` | Public Sites `RLV_desc`, by BBL |

`capitalneeds_unfunded` stays empty - City Hall's field mapping explicitly excluded it ("Strike,
not needed").

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
from `rezoning_area` to the ULURP number of the zoning map amendment that made it up (each area
maps to exactly one ULURP number today, but the crosswalk and the join both support more than
one per area). `dcp_zoningmapamendments` carries the actual mapped geometry per ULURP number -
one row per disjoint sub-area, since a single ULURP action can produce several unconnected
mapped areas. A BBL whose PLUTO lot `ST_Intersects` any sub-area of a rezoning area's ULURP
number(s) gets that area's commitments; areas with no ULURP number in the seed (the two citywide
rollups) are dropped before joining. This replaced an earlier NTA-based join
(`pluto.boroct2020 -> ct2020.boroct2020 -> ntaname`, matched against a hand-built
`rezoning_area` -> NTA crosswalk) that was too coarse - NTAs run far larger than the actual
rezoning boundaries. The tracker is a yearly progress snapshot - the same commitment recurs
across multiple years with an evolving `commitment_stage` - so the intermediate model first
collapses to each commitment's latest year before counting. `stg__hpd_rezoning_tracker` assigns
an artificial `commitment_id` (source has no usable one - see Limitations) that `poa_commitment`
uses for traceability.

**Public Sites for Housing** (`models/staging/stg__public_sites_for_housing.sql`): a plain
`LEFT JOIN ... ON lift.bbl = public_sites.bbl` - City Hall's tracker has a BBL column, so no
spatial join is needed. The field mapping (which source column feeds which LIFT column) came
from City Hall by email and is applied as given, naively, in `lift_supplemented.sql` - see
Limitations for the open questions to confirm with them.

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

**`poa`/`poa_commitment` are matched at the rezoning-area level, not the individual site
level.** A BBL gets *every* commitment tied to a rezoning area whose mapped geometry it falls
in, not commitments specific to that lot - the rezoning tracker has no finer-grained location
data than the study area itself. This is far tighter than the old NTA-based match (the mapped
rezoning boundary vs. the whole neighborhood it sits in), but read `poa`/`poa_commitment` as
"rezoning activity covering this BBL," not "commitments about this BBL."

**The rezoning tracker has no reliable id, so `commitment_id` is artificial.** The closest thing
to a source id, `map_order`, isn't stable: in 25 cases one `map_order` covers multiple different
`commitment_title`s within the same area, and titles drift across multiple `map_order` values
across years. `stg__hpd_rezoning_tracker` assigns `commitment_id` via
`DENSE_RANK() OVER (ORDER BY rezoning_area, commitment_title)` instead - stable within a build as
long as the distinct (area, title) set doesn't change, but it's ours, not the source's, and isn't
guaranteed stable across a source refresh that adds or removes commitments.

**`seeds/rezoning_areas.csv` is a manually-curated crosswalk, not authoritative.** Matches were
made by hand, pairing each `rezoning_area` with the ULURP number(s) of the zoning map
amendment(s) that make it up. The two citywide rollups (`COY: Economic Opportunity`, `COY:
Housing`) have no single ULURP action behind them and are left unmatched (`ulurp_no` blank)
rather than guessed. `dcp_nta2020` is no longer used anywhere in the pipeline - it's still an
input in `recipe.yml` as a leftover from the prior NTA-based crosswalk and can be dropped if
nothing else picks it up.

**The Public Sites for Housing field mapping is unconfirmed - raise these at the City Hall
meeting.** The mapping was supplied by email (2026-09-21) and applied naively, without back-and-
forth:
- `zzz_lift_data_zzz <- Site` is odd on its face: `ZZZ_LIFT_DATA_ZZZ` is documented elsewhere in
  this README as one of DCAS's section-boundary marker columns (`ZZZ_*_ZZZ`), not a real data
  field. Populating it with the Public Sites `Site` name is exactly what the email asked for, but
  worth confirming that's actually the intended target column.
- `redev_priority <- Priority` doesn't exist verbatim in the Public Sites spreadsheet. The closest
  field is `Prioritization` (there's also `Prioritization Sort`, `DCP Priorization`, and a
  `Prioritization` column per reviewing agency - `HPD Prioritization`, `EDC Prioritization`, etc.)
  - `Prioritization` was used as the best guess.
- **BBL is the join key** (not stated in the email, inferred from the data - the Public Sites
  sheet has its own `BBL ` column matching LIFT's grain). Of 379 source rows, 14 have no BBL and 7
  list multiple BBLs in one cell (a site spanning several tax lots, in a mix of comma/semicolon/
  dash-range formats) - none of these currently join. 5 BBLs appear on more than one row (distinct
  named sub-sites/proposals sharing one tax lot); `stg__public_sites_for_housing.sql` keeps only
  the first row per BBL to preserve `lift_supplemented`'s one-row-per-bbl grain, so the other
  sub-site's fields are silently dropped for those BBLs. Of ~352 rows with a usable BBL, only ~324
  match a LIFT bbl - the remaining Public Sites rows may reference city-owned sites outside the
  current LIFT extract.
- `capitalneeds_unfunded` was excluded per the email ("Strike, not needed") and is left empty.

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
