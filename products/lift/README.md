# LIFT

Land Inventory Fast Track (LIFT) is a multi-agency effort, established by Executive Order 4, to
identify City-owned land suitable for housing. This product produces the supplemented LIFT
dataset: DCAS sends an extract of City-controlled tax lots and we add DCP data on displacement risk,
capital project spending, census tract and NTA, and transit zone.

## Important files

[recipe](./recipe.yml)

[data_dictionary.csv](./data_dictionary.csv) - DCAS's field spec, generated from their workbook
by [scripts/update_data_dictionary.py](./scripts/update_data_dictionary.py). Rerun that script
when DCAS sends a new revision.

[data_issues.md](./data_issues.md) - known problems in LIFT's source data.

[maps/](./maps/README.md) - CARTO Builder map configs and how to load their data.

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
| `hpd_poa_commitments` | `edm-private` | HPD's Points of Agreement (POA) commitments tracker, by BBL |
| `dcp_housing_ahft` | `edm-private` | DCP's Affordable Housing Fair Share (AHFT) tracker - housing production stats and rank, by community district |
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
| `poa` | HPD POA commitments tracker's `Site` name for the BBL, or `NULL` - see Limitations, unconfirmed mapping |
| `poa_commitment` | HPD POA commitments tracker's free-text commitment narrative for the BBL, or `NULL` |
| `ahft` | `'Y'` if the BBL's community district is in the bottom 12 of 59 CDs by rate of affordable housing development (DCP's AHFT tracker), else `NULL` |
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

**POA commitments** (`models/staging/stg__hpd_poa_commitments.sql`): HPD's Points of Agreement
(POA) commitments tracker has a BBL column, so - like Public Sites below - this is a plain
`LEFT JOIN ... ON lift.bbl = poa_commitments.bbl`, no spatial match needed. This replaced an
earlier match through the NYC Rezoning Tracker (no BBL or geometry of its own - keyed by
`rezoning_area`, one of 13 named neighborhood-scale study areas, joined to a BBL via
`pluto.geom ST_Intersects` the mapped ULURP geometry of that area's zoning map amendment), which
gave a BBL *every* commitment tied to the rezoning area covering it rather than commitments
specific to that site. The new tracker is keyed directly to BBL, one row per site, so no such
over-matching or spatial join is needed. The field mapping (which source column feeds `poa` vs.
`poa_commitment`) is a naive best-effort guess - see Limitations.

**AHFT** (`models/staging/stg__dcp_housing_ahft.sql`): joined by community district, not bbl -
`LEFT JOIN ... ON lift.cd = ahft.borocd`. `lift_csv` already carries `boro`/`cd` directly from
DCAS's extract (`cd` is PLUTO's format: `BORO` int 1-5 * 100 + district, e.g. Brooklyn CD10 ->
`310`); AHFT's own `Community District` column uses a different format (2-letter borough
abbreviation + 2-digit district, e.g. `BK10`), so `stg__dcp_housing_ahft` derives `borocd` to
match LIFT's format instead of touching `stg__lift_csv`. `ahft_rank` (1 = lowest rate of
affordable housing development, e.g. Bay Ridge/BK10 is rank 1, up to 59 = highest) comes straight
from the source; `lift_supplemented` turns `ahft_rank <= 12` into `'Y'`/`NULL`. A dedicated test
(`tests/assert_ahft_cds_in_lift.sql`) checks every AHFT community district has a matching
`lift.cd` value, since the 59-row tracker covers the city's full CD set and a silent non-match
would most likely mean the key derivation broke.

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

**The HPD POA commitments field mapping is unconfirmed.** The 2026-09-29 extract has no
short/long split like the data dictionary implies (`poa` = "short description", `poa_commitment`
= "description of any previous POA commitments") - just one free-text narrative column
(`POA/Community`) plus a `Site` name. `stg__hpd_poa_commitments`/`lift_supplemented` map
`Site -> poa` and `POA/Community -> poa_commitment`, naively, without confirming with HPD.

**At least one BBL in the HPD POA commitments extract is malformed.** `400024007` (for "44-59
45th Avenue (LIC)") is only 9 digits - a standard BBL is boro(1) + block(5) + lot(4) = 10 - most
likely a dropped leading zero on the lot number somewhere upstream. `stg__hpd_poa_commitments`
doesn't try to guess the correct value, so this row silently fails to join to any LIFT bbl; worth
raising with HPD if/when the mapping above gets confirmed.

**`dcp_nta2020` is no longer used anywhere in the pipeline.** It's a leftover from the prior
NTA-based rezoning crosswalk (replaced, then removed entirely in favor of HPD's new BBL-keyed POA
commitments tracker) - still an input in `recipe.yml` and can be dropped if nothing else picks it
up.

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
dbt test --select assert_ahft_cds_in_lift
```

Or run the whole thing (load excluded) via:

```bash
./bash/build.sh
```

`dbt-duckdb` is a real dependency of `dcpy-lifecycle` now, so `uv sync --all-packages` picks it up.

### Export

`recipe.yml` declares these exports (see `exports:` at the bottom of the recipe):

- `lift_supplemented.csv`
- `lift.gdb.zip`: a FileGDB with `lift_supplemented` as a polygon layer (PLUTO lot geometry, via
  `lift_supplemented_map`). The layer's columns are retyped for mapping (see
  `lift_supplemented_map` in `_product_models.yml`), so it doesn't match the CSV column for
  column. FileGDB has no boolean type, so flags land as 0/1.
  It also has `community_districts` (via `community_districts_map`): community district
  boundaries with each district's AHFT rank, for the map's AHFT layer.

Run the export after the dbt build:

```bash
python3 -m dcpy lifecycle builds export export --recipe-path recipe.lock.yml
```

This writes the files above to `output/dataset_files/`, plus `output/output.zip`.

## Dagster

`apps/dagster/builds/assets.py` discovers every product under `products/` with a `recipe.yml`
and generates `plan_lift` / `build_lift` / `draft_lift` assets from it, so there's no
LIFT-specific Dagster code. `build_lift` runs each command in `recipe.yml`'s
`stage_config.builds.build.commands` in order, each as its own subprocess.

dcpy sets `DUCKDB_PATH` before each command to the same file the loader created.
