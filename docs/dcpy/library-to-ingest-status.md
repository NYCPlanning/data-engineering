# Library → Ingest: remaining work

Status snapshot as of 2026-07-12, built by diffing `dcpy/library/templates/*.yml` against
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
| `nycdoc_corrections`, `nycoc_checkbook`, `nycourts_courts` (issue lists this as `uscourts_courts` — likely a typo), `usfws_nyc_wetlands` | [#1533](https://github.com/NYCPlanning/data-engineering/issues/1533) — custom scripts |

## Remaining: untracked (no GitHub issue at all)

33 datasets that were never triaged into a sub-issue:

`cbbr_submissions`, `dcp_access_ADA_subway`, `dcp_access_subway_SBS`,
`dcp_beaches`, `dcp_censusdata_blocks`, `dcp_dot_trafficinjuries`,
`dcp_mappluto_clipped`, `dcp_mappluto_historical`, `dcp_mappluto_wi`, `dcp_pop_acs`,
`dcp_pop_decennial_ddhca`, `dcp_pop_decennial_dhc`, `dcp_pops`, `dcp_proximity_establishments`,
`dcp_trafficanalysiszones`, `ddc_capitalprojects_infrastructure`, `ddc_capitalprojects_publicbuildings`,
`dep_wwtc`, `dob_jobapplications_parkingspaces`, `dob_natural_resource_check_flags`,
`doitt_zipcodeboundaries`, `dot_bridgehouses`, `dot_ferryterminals`, `dot_projects_bridges`,
`edc_capitalprojects`, `edc_capitalprojects_ferry`, `fbop_corrections`, `fema_firms_500yr`,
`nysdoccs_corrections`, `nysed_activeinstitutions`,
`pluto_corrections`, `sca_bluebook`, `sca_capacity_projects_current`, `sca_e_pct`,
`sca_e_projections`, `usdot_ports`

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

| Old library template | New ingest template |
|---|---|
| `dob_jobapplications` | `dob_bis_applications` |
| `dob_cofos` | `dob_bis_cofos` |
| `dob_permitissuance` | `dob_bis_permits` |
| `fema_pfirms2015_100yr` | `dcp_pfirms` |

`moeo_socialservicesitelocations` is a similar case but 1-to-many, not 1-to-1: the old single
library template (a custom script joining 4 Socrata sources) became 4 separate ingest templates
(`moeo_socialservicesitelocations_{sites,contracts,providers,programs}`), joined back together in
FacDB's SQL layer (`products/facilities/facdb/sql/pipelines/moeo_socialservicesitelocations.sql`)
rather than in Python.
