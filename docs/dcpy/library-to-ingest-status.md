# Library → Ingest

The `dcpy.library` package is gone. Every dataset it archived either has an ingest template in
`ingest_templates/`, or a retired library template in `ingest_templates/library_deprecated/` (see its
README for why each one is there). Workflows archive with `dcpy lifecycle scripts ingest`.

Library's archives stay in `edm-recipes` and are read like any other archive, so read-side checks
like `_is_library` in `ingest_datastore.py` stay too.

[library-to-ingest-status.csv](./library-to-ingest-status.csv) has per-dataset archive state, from
`admin/ops/library_to_ingest_status_csv.py`. The sections below record how each group of datasets
was migrated and checked. The template counts in them predate the package's removal.

## Removing the package

With every dataset migrated, nothing outside `python/library/` needed the package:

- the QA app's "Data Ingestion" page uploaded a file and ran library on it. "Inbox Ingest" replaced
  it, so the page is gone.
- the Source Data page and two test files only used library's models. The tests read library-era
  configs with `SparseConfig`, which already handled both shapes.
- `ingest_or_library_archive` fell back to library when a dataset had no ingest template. No
  dataset needed that, so it became `dcpy lifecycle scripts ingest`.
- `validate_ingest` and the `ingest_library_compare`, `ingest_library_run_single`, and
  `data_library_single` workflows only existed to run library, as did
  `apps/dcpy_integration_tests/library/`.
- `usdot_ports` is still blocked on a token
  ([#1326](https://github.com/NYCPlanning/data-engineering/issues/1326)), so its library template
  moved to `library_deprecated/`. Facilities reads its last library archive.

The `GDAL` pin moved to `dcpy-geospatial`, which is what imports `osgeo`. It reaches the same
images as before, through `dcpy-lifecycle[geo]`.

## Migrated, but `latest` is still a library archive

4 datasets in the CSV (`migrated_to_ingest` true, `archived` true, `archived_via_ingest` false):

| Dataset | Why it's still library |
|---|---|
| `dcp_sfpsd` | The 2021 Bytes url its template downloads from returns 404. |
| `uscourts_courts` | The scraper's source returns 403 when run from GitHub Actions. |
| `nycoc_checkbook` | The template runs cleanly. Re-archiving is a data refresh for cpdb (2,475,570 to 2,542,210 rows, plus a new `expense_category_code` column), so it waits for the next cpdb update. |
| `doe_hszones` | The template runs cleanly (80 rows, same as library). Nothing reads it. |

If the source version hasn't changed, re-ingesting one fails on purpose with `FileExistsError`, and
replacing the archive takes `--overwrite --version <version>`.

`--overwrite` moves the library archive at that version to `<version>_library/` before writing.
It used to replace only the parquet and `config.json`, leaving library's `.sql` beside the new
parquet. Builds resolve `latest` to a version and pick a file type from that version's folder, so
anything that doesn't pin `file_type` kept loading library's pg_dump. Twelve datasets were left
that way and moved with `admin/ops/clear_library_files_from_latest.py --apply`: the cpdb six,
`dcp_beaches`, `dob_natural_resource_check_flags`, `dcp_waterfront_access_map_pow`,
`dcp_waterfront_access_map_wpaa`, `dcp_wrp_recognized_ecological_complexes`, and
`dcp_wrp_special_natural_waterfront_areas`.

## Retired without migrating

`dcp_pop_acs`, `dcp_pop_decennial_dhc`, and `dcp_pop_decennial_ddhca` moved to
`ingest_templates/library_deprecated/`. factfinder still reads them, but as whole Excel workbooks
it parses sheet by sheet, and library only archived them as-is. Their sheet layouts change between
deliveries, and the plan is to stop relying on these files, so per-sheet ingest templates weren't
worth building. A new delivery is uploaded straight to `edm-recipes` (`dcp_pop_acs/2024` already
was: it has no `config.yml`). See `products/factfinder/README.md`.

`dcp_dot_trafficinjuries` and `dob_jobapplications_parkingspaces` also moved there. edde pins
every traffic year from 2010 to 2020 and developments pins the parking spaces export at
`20240603`, all already archived, and neither has had a new delivery since.

## Migrated: facilities cluster

Nine datasets moved in one pass (PR #2608). Each new template was run locally against the same
source version and its output diffed against the archive it replaces:

| Dataset | Source type | Result |
|---|---|---|
| `fbop_corrections` | s3 inbox csv | 3 rows, columns and values identical |
| `nysdoccs_corrections` | s3 inbox csv | 44 rows, identical |
| `nysed_activeinstitutions` | s3 inbox csv | 7,368 rows, identical |
| `dep_wwtc` | s3 inbox csv | 14 rows, identical |
| `nycourts_courts` | s3 inbox csv | 14 rows, identical |
| `nycdoc_corrections` | custom scraper | 12 rows, identical |
| `dot_bridgehouses` | s3 inbox shapefile | 34 rows, attributes identical, geometry within 2.4 nm |
| `dot_ferryterminals` | s3 inbox shapefile | 2 rows, attributes identical, geometry within 1.6 nm |
| `dcp_pfirms` | arcgis feature server | see below |

Three things worth carrying forward:

- **pandas and ogr2ogr disagree on csv edge cases.** `fbop_corrections` has literal `"N/A"` values
  that pandas nulls by default (`keep_default_na: false` restores them), and
  `nysed_activeinstitutions` has a duplicate header that pandas dedupes as `popular_name.1` where
  ogr2ogr produced `popular_name2`. Diff against the archive rather than assuming a clean port.
- **Geometry column naming isn't uniform.** Library's parquet used `geometry`, its pg_dump used
  `wkb_geometry`, and ingest emits `geom` (`data_loader.py` sets `GEOMETRY_NAME=geom`, and
  geoparquet loads via `to_postgis`). Check what the consumer actually reads before switching.
- **A hand-maintained source needs a guard.** `nycdoc_corrections` matches facility names against a
  hardcoded list; six had silently stopped matching as facilities closed. It now raises instead of
  returning fewer rows.

## Migrated: cpdb cluster

Six datasets, all read only by cpdb. Each new template was run locally against the version in
`latest` and diffed against that library archive:

| Dataset | Source | Result |
|---|---|---|
| `ddc_capitalprojects_infrastructure` | s3 inbox shapefile | 70,652 rows, attributes identical, geometry within 2e-8 ft |
| `ddc_capitalprojects_publicbuildings` | s3 inbox shapefile | 6,860 rows, attributes identical, geometry within 2e-8 ft |
| `dot_projects_bridges` | s3 inbox shapefile | 127 rows, attributes identical, geometry within 1e-8 ft |
| `edc_capitalprojects` | s3 inbox shapefile | 19 rows, attributes identical, geometry within 1e-8 ft |
| `edc_capitalprojects_ferry` | library's own shapefile export | 1 row, identical to the pg_dump |
| `dcp_trafficanalysiszones` | library's own shapefile export | 2,243 rows, identical to the pg_dump |

Things worth carrying forward:

- **Library forced multi-part geometry, and cpdb depends on it.** Its product models filter on
  `ST_GeometryType(geom) = 'ST_MultiPolygon'`, so these templates use the `multi` step. Shapefiles
  read through ingest otherwise come out as a mix of single and multi.
- **`ogc_fid` only exists in library's pg_dump.** Ogr2ogr adds it as a serial key on load.
  `products/cpdb/python/dot_bridges.py` was recasting it but nothing read it, so that line is gone.
- **Two sources had no raw file.** `edc_capitalprojects_ferry` and `dcp_trafficanalysiszones` were
  ingested by library from a local `.library/upload/*.csv` that was never kept. Their inbox copies
  are library's `.shp.zip` export with the `geom` column dropped. That column was the csv's
  geometry as hex EWKB text, cut off at the shapefile's 254 character limit, and it collides with
  ingest's own `geom`.
- **Date columns change type.** The DDC shapefiles have seven date fields that library loaded as
  `date` and ingest reads as timestamps. cpdb doesn't read any of them.

After re-ingesting all six, a cpdb build on main loaded their parquets and matched the
2026-11-08 `nightly_qa` build, which had the same source versions but loaded these six from
library's pg_dumps. `cpdb_projects`, `cpdb_projects_boundaries` (12,344 rows), `cpdb_adminbounds`
(98,834 rows), and both mapping summaries are identical. `cpdb_dcpattributes_poly` and
`_pts` match on every attribute, with geometry within 1e-8 ft.

## Migrated: ceqr and green_fast_track pair

| Dataset | Source | Result |
|---|---|---|
| `dcp_beaches` | s3 inbox shapefile | 50 rows, attributes identical, geometry identical in 2D |
| `dob_natural_resource_check_flags` | library's own csv export | 42,851 rows, every cell identical |

- **`dcp_beaches` drops its z.** The shapefile is PolygonZ with every z at 0, and library's parquet
  mixed 3D and 2D rows, which is why ceqr pinned it to `pg_dump`. The template applies
  `force_2d`, matching the 2D pg_dump, and the pin is gone. Its geometry column stays `geometry`
  because green_fast_track reads the parquet directly and already expects that name.
- **`dob_natural_resource_check_flags` reads every column as text.** Boro, block, lot, and bbl
  are zero-padded codes, and library's ogr2ogr csv read kept them as strings too.

## Re-archived through ingest

These had templates but their `latest` was still library's output. Each template was first run
through `ingest_single` into the `de-dev-d` bucket and diffed against the library archive, then
archived to `edm-recipes`, with `--overwrite` where the version was unchanged.

| Dataset | Version | Read by | Result |
|---|---|---|---|
| `fbop_corrections`, `nysdoccs_corrections`, `nysed_activeinstitutions` | unchanged | facilities | every value identical |
| `dot_bridgehouses`, `dot_ferryterminals` | `20240805` | facilities | every value identical. `bbl`, `block`, and `lot` are double instead of decimal |
| `nycdoc_corrections` | `20261109` | facilities | 12 rows, same columns |
| `dcla_culturalinstitutions` | `20210625` to `20260610` | facilities | 2,308 to 2,535 rows. `main_phone__` is now `main_phone_#`, which facilities doesn't read |
| `sca_enrollment_capacity` | `20230105` to `20260910` | facilities | 17,622 to 20,754 rows, same columns |
| 4 × `dcp_wrp_*` | `20200701` | ceqr | same rows. ceqr exports them without naming the geometry column |
| `dcp_zoningmapindex` | `20190701` to `20261109` | pluto, ztl | 131 rows. Keeps `wkb_geometry` |
| `dpr_park_access_zone` | `20230719` | factfinder | 1 row. Keeps `wkb_geometry` |
| `dcp_mappluto_clipped` | `26v2` | ceqr | 856,687 rows. cdbg pins library's `24v4` |
| `doe_eszones`, `doe_mszones` | `20240313` | developments, knownprojects | 770 and 279 rows |

- **Check geometry names against library's pg_dump, not its parquet.** Library's parquet named
  the column `geometry` and its pg_dump `wkb_geometry`, and any consumer that doesn't pin
  `file_type` loads the pg_dump. The school zones came out of ingest as `geom`, so developments
  and knownprojects were switched to `geom` before archiving
  ([#2935](https://github.com/NYCPlanning/data-engineering/pull/2935)).
- **Shapefile and GeoJSON polygons need `multi`.** `dcp_mappluto_clipped` and the school zones
  came out as a mix of Polygon and MultiPolygon until the step was added. Library's were all
  multi.
- **Facilities pinned `dcla_culturalinstitutions` to `csv`.** Its SQL built geometry from the
  `wkt` column in library's csv export. With the pin dropped and the SQL reading `geom`, a
  facilities build loaded all five facilities inputs above from parquet and passed.
- **`council_members` was retired.** Its Socrata dataset is now every council member since 1999,
  with different columns, and nothing reads it.

## Migrated: developments scripts, part 1

Four of #1336's six. Each ran through ingest at the version DevDB 26Q2.1 loaded and was compared
with that library archive.

| Dataset | Version | Result |
|---|---|---|
| `dcp_censusdata` | `2020` | 2,717 rows, all 232 columns identical |
| `dcp_censusdata_blocks` | `2020` | 37,990 rows, every 2020 block's `hunits` identical (3,618,635 total) |
| `dob_now_permits` | `20260630` | 791,611 rows, identical apart from number formatting |
| `dob_cofos` | `20260107` | 331,258 rows, identical apart from number formatting |

- **The census blocks workbook changed layout.** It used to put 2010 and 2020 side by side on one
  sheet, and library read the 2020 half, so pandas suffixed those columns with `.1`. It now has a
  sheet per year, like the core geographies workbook, so ingest reads the `2020` sheet whole:
  230 columns instead of 41. Developments reads `geoid` and `hunits` instead of `geoid20` and
  `"hunits.1"`.
- **Library read numbers as floats.** Its scripts loaded csvs and workbooks without `dtype=str`,
  so columns with blanks came out as `25.0`. Ingest keeps the text as delivered. Developments
  casts every one of these it reads.
- **`dob_cofos` was compared locally, not archived.** Each version appends to the previous one, so
  rebuilding `20260107` means appending to `20251025`, while the template appends to `latest`.
  The next delivery is its first archive through ingest.

With the other three archived, developments and knownprojects builds on the branch passed.

## Migrated: dob_now_applications

The fifth of #1336's six. DOB's export is `|`-delimited but doesn't escape `|` typed into free-text
fields, which shifts every later column. Library's script repaired those rows while reading. The
csv format now takes a `repair_unescaped_delimiters` option that does the same thing, with the
free-text columns and the checks on anchor columns set in the template.

Ran through ingest at `20260630` and compared with that library archive: 1,002,981 rows on both
sides, the same job numbers, and the same values in every column apart from number formatting
(library parsed numbers as floats, and latitude, longitude, and floor area differ by less than
1e-10). The read repaired 75 rows and dropped none.

With `20260630` archived through ingest, a developments build on the branch passed.

## Migrated: hpd_historical_units_by_building

The last of #1336's six. HPD's housing team sent this workbook once, in June 2023, and library
read it from a local upload folder. The original file is now in `edm-private`, so ingest can read
it in CI.

Ran through ingest at `20230630` and compared with that library archive: 14,070 rows and the same
11 columns, identical apart from `affordable_units`, which library wrote as floats (`52.0`).

## Migrated: zap exports

The seven tables `zap_export.yml` pulls from the ZAP CRM each week, which it used to archive with
`library archive` against templates in `products/zap-opendata/templates/`. Their ingest templates
point at the copies the workflow writes to `edm-private`, so `ingest_single` can re-archive a past
week. The weekly workflow passes `--local-file-path` to archive the file it just wrote.

Each ran through ingest at `20261109` from `edm-private` and was compared with that library
archive. All seven have the same rows and columns. Five are identical. In `dcp_projects` and
`dcp_projectactions`, two rows each differ only in line endings: the CRM stores `\r\n`, which
ingest keeps and library converted to `\n`. pandas reads `NA`, `N/A`, and `n/a` as null by
default, which would have nulled real values like `ceqr_number = 'N/A'`, so the templates treat
only blanks as null.

A dev run of `zap_export.yml` on the branch archived all seven to `de-dev-d` through the new step.

## Migrated: ceqr school inputs

The six SCA inputs to the four CEQR school datasets built each year for Capital Planning (see
[#2082](https://github.com/NYCPlanning/data-engineering/issues/2082)): `sca_bluebook`,
`sca_capacity_projects_current`, `sca_e_pct`, `sca_e_projections`, and the frozen
`sca_capacity_projects_prev` and `sca_capacity_projects_tcu`. They had been listed here as retired,
because no workflow runs the `ceqr_app` recipes that read them, but those recipes are run by hand
each year.

The recipes read these from the old `recipe` database, where each delivery was loaded directly, so
`edm-recipes` stops at `20241226` and the latest versions were never archived. Each was exported from
that database at the version its `latest` view pointed to, uploaded to `inbox/sca/`, and run through
ingest. All six match the database cell for cell.

The four recipes now build from those archives (plus `doe_lcgms`, already ingested) in a
`ceqr_schools.yml` workflow, instead of locally against the `recipe` database. A dev run matched the
2025-12-29 outputs for `sca_capacity_projects` (49 rows, same points), `sca_e_projections_by_boro`
(55), and `sca_e_projections_by_sd` (1,056). `ceqr_school_buildings` came out with 1,505 rows. The
2025-01-15 and 2025-12-29 releases of it were empty, because the local run lost every row in its
`docker run` geocoding step, while 2023 and 2024 had 1,500 and 1,503.

## Migrated: usfws_nyc_wetlands

Library's script downloaded the National Wetlands Inventory's zip for each of four HU8 watersheds
that contain NYC and concatenated their wetlands layers. Ingest takes one source per dataset, so
that loop moved into a `usfws_wetlands` connector. The statewide zip would have been one plain
`file_download`, but it's 612 MB against about 104 MB for the four watersheds, and every ceqr and
gft build would load and clip all of New York State.

Run into `de-dev-d` at `20261001`, the date USFWS last published the zips, and compared with
library's `20260908`: 62,520 rows each, the same `nwi_id`, `attribute`, and `wetland_type` values,
EPSG:2263 MultiPolygons, and total area within 2 sq ft of 40.45 billion. The source has a new
`qaqc_code` column, null on every row.

A gft build on the archived `20261001` passed with the same warnings as earlier builds. That
build also needed `dpr_forever_wild` pinned to `20261015`: Socrata's `48va-85tp` has since become a
different dataset (111 natural areas, with `propid` and `name`), and its export puts the polygons
near Florida while labeling them EPSG:4326. gft and ceqr stay pinned until that's fixed upstream.

## Migrated but not yet empirically validated

Both required infra this session didn't have access to (a local Postgres / dev container for
`run_and_compare`, and Geosupport's binary, which doesn't run on macOS at all). Full commit
messages have the details.

- `moeo_socialservicesitelocations` — the new SQL join (`moeo_socialservicesitelocations.sql`)
  deliberately sources `agency_name`, `bin`, and `bbl` from the `sites` sub-table wherever a column
  name collides across sites/contracts/providers/programs. This is a behavior change: the old
  pandas-merge version likely sourced `agency_name` from `programs` instead, an apparent accident of
  merge-suffix collision order, not a deliberate choice. Confirm downstream consumers are fine with
  this before/after comparing build output.
- `dcp_facilities` — extraction/transform verified locally against the real source (34,446 rows,
  correct EPSG:4326 reprojection, matches `dcp_facilities_with_unmapped`'s column schema). Not yet
  validated: an actual DB load/build for its 3 consumers (`cbbr`, `ceqr`, `cpdb`), and confirming
  `ceqr/recipe.yml` dropping its `file_type: pg_dump` override doesn't break anything — that
  override existed specifically because library's parquet output was missing a CRS definition,
  which ingest's doesn't have (verified locally), but it's only been checked at the file level, not
  through an actual `ceqr` build.

## Migrated and empirically validated

- `dep_cats_permits` — the ported Geosupport geocoding
  (`dcpy/lifecycle/scripts/dep_cats_permits_geocode.py`) ran for real in CI
  ([run 29431419121](https://github.com/NYCPlanning/data-engineering/actions/runs/29431419121),
  against the `nycplanning/build-geosupport` image with the real `1B`/`2`/`3` Geosupport calls, not
  mocked), archiving 17,622 rows from a 91,531-row raw extract. Compared against the `edm-recipes`
  duckdb catalog's historical library-pipeline output for this dataset (`2023-07-01`: 17,794 rows,
  `2024-06-01`: 17,557 rows, `2026-01-01`: 18,093 rows) — the new run's row count falls within that
  same historical band, confirming the raw→final drop is this dataset's normal filter/geocode ratio,
  not a regression. `applicationid`/`status`/`geom` — the only columns any downstream consumer
  (`ceqr`, `green_fast_track`) actually reads or dbt-tests — are unique/not-null/not-null as
  required. `geo_address` (always `None` in the old pipeline too — never actually populated) was
  added back for schema parity with the old output.

- `dcp_air_quality_vent_towers`, `dcm_arterial_highways`, `panynj_jfk_65db`, `panynj_lga_65db` — the
  rest of `green_fast_track`'s Air/Noise cluster (alongside `dep_cats_permits`). All 4 are simple
  single-shapefile-to-geometry conversions sourced from `s3://edm-recipes/inbox/...` (manually
  refreshed, not a live URL — kept as-is rather than switched to a different source type). Run
  locally against the actual raw shapefiles (`dcp_air_quality_vent_towers` needed the S3 key
  corrected to `inbox/dcp/dcp_air_quality_vent_towers/...`, which the old library template had
  wrong — the raw file was never at the path it declared) and compared against the `edm-recipes`
  duckdb catalog's historical library-pipeline output for the same version: row counts match
  exactly (vent towers 10/10, arterial highways 740/740, both panynj contours 1/1), as do all
  columns other than the geometry rename (`geometry` → `wkb_geometry`) every other template already
  uses, and — for the panynj pair — dropping the redundant raw `wkt` text column that duplicated
  the geometry column and that no downstream consumer reads. Checked the dbt source tests in
  `green_fast_track/models/_sources.yml` directly against the new output: `name`/`wkb_geometry`
  unique+not-null (vent towers), `wkb_geometry` unique+not-null and `name` not-null (arterial
  highways, where `name` is legitimately non-unique — multiple segments share a route name), and
  `wkb_geometry` unique+not-null (both panynj contours) all pass.

## Known false positives when re-running this diff

These library templates were actually migrated, just under a new `ingest_templates` filename, so a
naive filename diff wrongly flags them as remaining:

| Old library template | New ingest template | Consumers cut over? |
|---|---|---|
| `dob_jobapplications` | `dob_bis_applications` | yes |
| `dob_permitissuance` | `dob_bis_permits` | yes |
| `fema_pfirms2015_100yr` | `dcp_pfirms` | yes — library template deleted |

A recipe naming the old dataset reads whatever is archived under that name, and archiving it falls
back to library (`ingest_or_library_archive.py` keys off whether `ingest_templates/{dataset_id}.yml`
exists), so a half-finished rename keeps the old template on a live path.

### `dob_cofos` is not a rename

`dob_bis_cofos` is a narrower dataset, not the same data under a new name, so
`products/developments/recipe.yml:48` should stay on `dob_cofos` until a real replacement exists.
Compared `dob_cofos/20260107` against `dob_bis_cofos/20260103` in `edm-recipes`:

| | `dob_cofos` | `dob_bis_cofos` |
|---|---|---|
| Source | manual CSV by email, appended to `latest` | Socrata `bs8b-p36w` |
| Rows | 331,258 | 142,415 |
| Coverage before 2012 | 119,435 rows | none (starts 7/12/2012) |
| Certificates in 2025 | 15,938 | 1,376 |

The two track each other from 2013 to 2020, then diverge: BIS stopped being the system of record for
new COs around March 2021, and DOB NOW took over. `dob_bis_cofos`'s post-2021 rows are late
certificates on old BIS jobs, not current completions.

DevDB reads `jobnum`, `effectivedate`, `numofdwellingunits`, and `certificatetype`
(`products/developments/sql/_co.sql`). None of the four exist in `dob_bis_cofos`, and the closest
equivalents aren't equivalent: `numofdwellingunits` is one number, while the Socrata dataset splits
proposed and existing (`pr_dwelling_unit` / `ex_dwelling_unit`).

Migrating this dataset means answering where post-2021 COs come from (presumably a DOB NOW CO
dataset, which DevDB's recipe doesn't currently carry) and where the pre-2012 history comes from.
That's a DevDB data question, not a template port.

### `dcp_pfirms` — done

PLUTO and factfinder both read `dcp_pfirms` now, and
`python/library/dcpy/library/templates/fema_pfirms2015_100yr.yml` is deleted.

The two snapshots agree on every attribute: 4,914 rows each, identical `fld_zone` distribution,
identical 3,984 distinct `fld_ar_id`, total `area` matching to the unit. Neither consumer pinned a
version, so both resolved `latest`. What differed:

- **Geometry column.** Library emitted `wkb_geometry`; ingest emits `geom`.
- **Shapefile name truncation.** `shape_star` / `shape_stle` became `shape__area` / `shape__length`.
  No consumer reads them.

factfinder needed more than a rename. Its geolookup build loaded through `import_recipe`
(`bash/utils.sh`), which downloads `{id}.sql` and runs it — and ingest archives parquet, so there was
nothing for it to fetch. Geolookup now plans and loads through `dcpy.lifecycle.builds` instead, the
same path `products/factfinder/run.py` already used for acs and decennial. Because the loader prefers
a pg_dump when one exists (`RECIPE_FILE_TYPE_PREFERENCE`), the four datasets that still have `.sql`
archives load exactly as before; only `dcp_pfirms` comes in as parquet, which is why `build.sql`
aliases `geom AS wkb_geometry` for that one table and leaves the rest alone.

Two things this surfaced, neither addressed here:

- **`dpr_park_access_zone` is a latent break.** It has no library template anymore, but its `latest`
  is still the library-era `20230719` archive with a `.sql` in it. The next ingest run makes `latest`
  parquet-only. That would have broken geolookup silently on the old loader; on the new one it just
  works.
- **The CRS the template requests matters.** Sourcing this layer in 4326 silently moved it a meter
  away from PLUTO's own lot geometry. Detail below.

#### Why the template asks Esri for EPSG:2263

`ingest_templates/dcp_pfirms.yml` sets `crs: EPSG:2263` on the source and `target_crs: EPSG:4326`.
That looks redundant and isn't: **removing it reintroduces a one-meter datum shift.**

NAD83 and WGS84 are genuinely different datums, about a meter apart in this region. PROJ — used by
both GDAL and geopandas — resolves NAD83 to WGS84 with `NAD83 to WGS 84 (1)`, a null transformation
with a stated accuracy of 4 m; it applies a literal zero shift. Esri's feature service applies a real
transformation (`WGS_1984_(ITRF00)_To_NAD_1983`, stated accuracy 0.1 m). So asking the service for
4326 returns coordinates about 0.92 m north of what PROJ produces from the same source.

Esri's is the more accurate answer in absolute terms. It is still the wrong one here, because
`pfirm15_flag` is a spatial relationship between two layers, and PLUTO's lots reach 4326 through
PROJ: `dof_dtm` declares `crs: EPSG:2263` with `target_crs: EPSG:4326`, so the lots sit in the
null-transform frame along with every other PROJ-reprojected input. Improving one layer's absolute
accuracy while the lots stay put creates a one-meter *relative* error where there was none.

Measured, before the fix: every polygon displaced 0.920–0.929 m on bearing 352°, standard deviation
2.6 mm — a pure translation, not noise. It flipped `pfirm15_flag` on 554 of 858,056 lots (292 gained,
262 lost) while `firm07_flag`, `lotarea` and row counts stayed identical.

After sourcing in 2263, a PLUTO build on this branch against `nightly_qa` differs on **zero** lots,
with `dcp_pfirms` at `20260316` on one side and library's `fema_pfirms2015_100yr` at `20181219` on
the other. Different dataset, different pipeline, identical published output.

Moving PLUTO to true WGS84 remains a legitimate option, but it is a project-wide datum decision that
has to move `dof_dtm` and the lot geometry too — not something one input should do on its own.

Two source characteristics, neither introduced by the migration: the ArcGIS service returns null
geometry for two zero-area slivers (`fld_ar_id` 154 and 1335, both `VE`), and it emits `Polygon`
where library emitted `MultiPolygon`.

`moeo_socialservicesitelocations` is a similar case but 1-to-many, not 1-to-1: the old single
library template (a custom script joining 4 Socrata sources) became 4 separate ingest templates
(`moeo_socialservicesitelocations_{sites,contracts,providers,programs}`), joined back together in
FacDB's SQL layer (`products/facilities/facdb/sql/pipelines/moeo_socialservicesitelocations.sql`)
rather than in Python.
