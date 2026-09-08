# Library → Ingest: remaining work

Status snapshot as of 2026-09-06, built by diffing `dcpy/library/templates/*.yml` against
`ingest_templates/*.yml` directly — **not** from [tracking issue #1255](https://github.com/NYCPlanning/data-engineering/issues/1255)
or its sub-issues, which have drifted out of sync with the actual file state (see
[Known false positives](#known-false-positives-when-re-running-this-diff) below if you re-derive this list).

See [library-to-ingest-migration.md](./library-to-ingest-migration.md) for the how-to on migrating
a dataset. This doc only tracks *what's* left, not *how*.

## How this list was built

```bash
comm -23 <(ls dcpy/library/templates | sed 's/\.yml$//' | sort) \
         <(ls ingest_templates | sed 's/\.yml$//' | sort)
```

A dataset counts as migrated once `ingest_templates/{dataset_id}.yml` exists — that's the literal
fallback check in `dcpy/lifecycle/scripts/ingest_or_library_archive.py`. Filename diffs alone
overcount "remaining," because several datasets were renamed during migration; those are excluded
below (see [Known false positives](#known-false-positives-when-re-running-this-diff)).

`test_nypl_libraries` is a unit-test fixture (`dcpy/test/library/data/`), not a production
dataset, and isn't counted.

## Remaining: tracked in an open GitHub issue

| Dataset(s) | Issue |
|---|---|
| `dcp_censusdata`, `dob_now_applications`, `dob_now_permits`, `hpd_historical_units_by_building` | [#1336](https://github.com/NYCPlanning/data-engineering/issues/1336) — DevDB script sources |
| `nycoc_checkbook`, `usfws_nyc_wetlands` | [#1533](https://github.com/NYCPlanning/data-engineering/issues/1533) — custom scripts. `nycdoc_corrections` and `nycourts_courts` are done; `uscourts_courts` in that issue is a separate dataset, not a typo for `nycourts_courts` — facilities consumes both |
| `usdot_ports` | [#1326](https://github.com/NYCPlanning/data-engineering/issues/1326) — source now requires a token, so there is nothing to migrate to yet |

## Remaining: untracked (no GitHub issue at all)

30 datasets that were never triaged into a sub-issue:

`cbbr_submissions`, `dcp_access_ADA_subway`, `dcp_access_subway_SBS`,
`dcp_beaches`, `dcp_censusdata_blocks`, `dcp_dot_trafficinjuries`,
`dcp_mappluto_clipped`, `dcp_mappluto_historical`, `dcp_mappluto_wi`, `dcp_pop_acs`,
`dcp_pop_decennial_ddhca`, `dcp_pop_decennial_dhc`, `dcp_pops`, `dcp_proximity_establishments`,
`dcp_trafficanalysiszones`, `ddc_capitalprojects_infrastructure`, `ddc_capitalprojects_publicbuildings`,
`dob_jobapplications_parkingspaces`, `dob_natural_resource_check_flags`,
`doitt_zipcodeboundaries`, `dot_projects_bridges`,
`edc_capitalprojects`, `edc_capitalprojects_ferry`,
`fema_firms_500yr`, `fema_firms2007_100yr`,
`pluto_corrections`, `sca_bluebook`, `sca_capacity_projects_current`, `sca_e_pct`,
`sca_e_projections`

### Triage: which of those are still consumed

Consumers were found by grepping each dataset name across `products/*/*.yml`, `*.sql`, and `*.py`.
Note that a `recipe.yml`-only scan is not enough: factfinder loads its inputs from `acs.yml` and
`decennial.yml` instead, so the three `dcp_pop_*` datasets look dead to a recipe scan and aren't.

**Dead — delete rather than migrate (7).** No build, workflow, or query reads these.

| Dataset | Evidence |
|---|---|
| `cbbr_submissions` | CBBR's recipe reads `dcp_cbbr_requests` and `omb_cbbr_agency_responses`. The `_cbbr_submissions` hits under `products/cbbr/` are an internal build table, not this dataset. |
| `dcp_access_ADA_subway` | No reference outside its own template. |
| `dcp_access_subway_SBS` | No reference outside its own template. |
| `dcp_proximity_establishments` | No reference outside its own template. |
| `fema_firms_500yr` | No reference outside its own template. Its siblings `fema_firms2007_100yr` and `dcp_pfirms` are both live in PLUTO; this one isn't. |
| `dcp_mappluto_historical` | Only `.github/workflows/archive/pluto_publish_historical.yml`, an archived workflow. |
| `pluto_corrections` | Every reference is commented out (`corr_lotarea.sql:28`, `archive/pluto_publish.yml:45,60,70`). |

**Dormant (4).** `sca_bluebook`, `sca_capacity_projects_current`, `sca_e_pct`, `sca_e_projections`
are read only by `products/ceqr/ceqr_app/recipes/`. No workflow runs those recipes:
`ceqr_dep_monthly.yml` is the only one pointing at `ceqr_app`, and it runs `dep_cats_permits` alone.
Same shape as the `doe_pepmeetingurls` deletion above, so confirm with the CEQR owner before
choosing delete over migrate.

**Live (19).** Grouped by consuming product, most-shared first.

| Consumers | Datasets |
|---|---|
| 6 (`cpdb`, `developments`, `facilities`, `green_fast_track`, `knownprojects`, `lift`) | `dcp_mappluto_wi` |
| 3 (`developments`, `facilities`, `pluto`) | `doitt_zipcodeboundaries` |
| 3 (`ceqr`, `facilities`, `green_fast_track`) | `dcp_pops` |
| 2 (`ceqr`, `green_fast_track`) | `dcp_beaches`, `dob_natural_resource_check_flags` |
| 2 (`cdbg`, `ceqr`) | `dcp_mappluto_clipped` |
| 1 (`cpdb`) | `dcp_trafficanalysiszones`, `ddc_capitalprojects_infrastructure`, `ddc_capitalprojects_publicbuildings`, `dot_projects_bridges`, `edc_capitalprojects`, `edc_capitalprojects_ferry` |
| 1 (`factfinder`, via `acs.yml` / `decennial.yml`) | `dcp_pop_acs`, `dcp_pop_decennial_ddhca`, `dcp_pop_decennial_dhc` |
| 1 (`developments`) | `dcp_censusdata_blocks`, `dob_jobapplications_parkingspaces` |
| 1 (`pluto`) | `fema_firms2007_100yr` |
| 1 (`edde`) | `dcp_dot_trafficinjuries` |

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

## Deletion candidates (not migration candidates)

- `dycd_afterschoolprograms` — a comment on [#1267](https://github.com/NYCPlanning/data-engineering/issues/1267)
  says it looks unused, likely superseded by `dycd_service_sites`. Confirm it's actually dead (no
  build references it) before deleting the library template outright.
- ~~`doe_pepmeetingurls`~~ — deleted (template, dev_templates stub, and script). Only used to
  refresh the source for CEQR's `doe_significant_utilization_changes` recipe, which is
  pretty-much-deprecated and reads already-archived data, not a live template — so deleting the
  refresh path doesn't break it.

## Known false positives when re-running this diff

These library templates were actually migrated, just under a new `ingest_templates` filename, so a
naive filename diff wrongly flags them as remaining:

| Old library template | New ingest template | Consumers cut over? |
|---|---|---|
| `dob_jobapplications` | `dob_bis_applications` | yes |
| `dob_permitissuance` | `dob_bis_permits` | yes |
| `dob_cofos` | `dob_bis_cofos` | **no, and it shouldn't be** — see below |
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
`dcpy/library/templates/fema_pfirms2015_100yr.yml` is deleted.

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
