# Green Fast Track — DuckDB Port Issues

Bugs and gaps found while porting `green_fast_track`'s build (and, more recently, export) from
Postgres to DuckDB. This is a working list, not the formal `data_issues.md` format cscl uses —
just enough to not lose track of what's fixed, what's fixed-but-elsewhere, and what's still open.

## Fixed on `ar-gft-duckdb-2026`

- **Un-reprojected geometry treated as already-2263.** `int_spatial__vent_tower`,
  `stg__dcm_arterial_highways`, and `stg__panynj_airports` buffered/used raw source geometry
  assuming it was already state-plane feet (true for the postgres archive convention, false for
  these three datasets' native-WGS84 duckdb parquet archives) — buffering degree-scale
  coordinates by "75 feet" produced garbage polygons matching nothing downstream.
- **`SIMILAR TO 'PA%|PB%'`** is valid syntax in both dialects but means something different —
  duckdb doesn't treat `%` as a LIKE-style wildcard there. Was silently zeroing out all CATS
  permit flags. Replaced with `LIKE`.
- **`bbl::text` with no `AS bbl`** drops the column name in duckdb (kept in postgres), breaking
  every downstream join on `stg__pluto.bbl`.
- **9 datasets have a different geometry column name** in their duckdb parquet archive than in
  the postgres pg_dump (`wkb_geometry` vs `geom`/`geometry`) — handled via a `dcp_geom_column()`
  mapping macro.
- **`ST_HEXAGONGRID`/`ST_ESTIMATEDEXTENT`** (postgres perf hack for tiling huge polygons before a
  spatial join) has no duckdb equivalent — replaced with a direct join plus an RTREE index, since
  duckdb's spatial join already does its own bounding-box pruning.
- **Two OOM walls** (`int_flags__spatial`, `green_fast_track_bbls`), fixed by restructuring the
  queries, not just tuning memory settings: `GROUP BY + MIN` instead of `DISTINCT`/`ROW_NUMBER`
  over full geometry blobs; splitting one 22-column concurrent `array_agg`/`FILTER` aggregate into
  22 sequential per-flag aggregations joined together (duckdb doesn't share the underlying grouped
  scan across `FILTER`'d aggregates the way postgres does — memory scaled ~linearly per column).
- **Renames with no direct equivalent:** `ST_UNION` → `ST_UNION_AGG`, `ST_RELATE` (DE-9IM) →
  `ST_INTERSECTS AND NOT ST_TOUCHES`, `geometrytype` → `ST_GeometryType`, `REGEXP_MATCH` →
  `REGEXP_EXTRACT`, nested `UNNEST` restructured, `DISTINCT ON` → plain aggregation, dropped
  postgres-only `indexes:` model config.
- **`ST_GEOMETRYTYPE(...) = 'ST_MultiPoint'`-style comparisons** (8 files under
  `models/product/source/`) used postgis's `ST_`-prefixed type names; duckdb's `ST_GeometryType`
  returns plain `'MULTIPOINT'`/`'MULTIPOLYGON'`/`'MULTILINESTRING'`. Silently zeroed out several
  export layers (`natural_resources_*`, `shadow_*`) — found while wiring up exports.
- **`dcp_mappluto_wi.bbl` is `DOUBLE`** in the duckdb parquet archive. `bbl::text` casts
  `3062640072.0` to the literal string `"3062640072.0"` (trailing `.0`); the join target built
  from `dob_natural_resource_check_flags` has no decimal. This silently zeroed
  `int_flags__dob_natural_resources` (0 rows) for the entire session until caught while
  investigating an empty export layer. Fixed via `CAST(bbl AS BIGINT)::text` in `stg__pluto.sql`.
- **`dcpy.lifecycle.builds.export` had no DuckDB geodataset (shapefile/gdb) support at all** —
  implemented `DuckDBClient.read_table_gdf`/`read_table_df`, `export_geodataset_from_duckdb`, and
  extended the gdb-layer-grouping logic in `export()` to work for either backend.
- **`usnnum || COALESCE('-' || usnname, '')` leaves a trailing `-`** on ~18k
  `nysshpo_historic_buildings_points`/`_polygons` rows in duckdb. `usnname` is `NULL` in the
  postgres archive for these records but `''` (empty string) in duckdb's parquet archive for the
  same underlying rows — an archive-ingestion divergence, not a duckdb SQL issue per se. Found by
  diffing `(variable_id, bbl)` pairs against a live Postgres `main`-schema build. Fixed with
  `NULLIF(usnname, '')` in `stg__nysshpo_historic_buildings.sql` and
  `stg__nysshpo_historic_building_districts.sql`, which normalizes both conventions the same way.
- **`source__shadow_hist_resources_lots.sql` was missing a `WHERE lot_geom IS NOT NULL` filter**
  that its two sibling models (`source__historic_resources_lots.sql`,
  `source__historic_resources_adj_lots.sql`) both have — all three derive `lot_geom` from the same
  landmarks-to-PLUTO join, where ~1,407 landmarks don't match any lot. Without the filter, the
  exported GDB layer carried 1,407 null-geometry features in a nominally "Multi Polygon" layer
  (89,829 rows vs the siblings' 88,422). Pre-existing in hand-written SQL, unrelated to the
  duckdb port — found by validating the exported `.gdb` with `ogrinfo` and cross-checking every
  layer's feature count against the live duckdb tables (this was the only mismatch out of 41
  layers). Fixed by adding the same filter; re-validated with `ogrinfo`, all 41 layers now match
  exactly.
- **`dcpy` export path ignored `BUILD_ENV_OUTPUT_DIR`** (unlike `load.py`), so export couldn't
  find where load actually put the duckdb file in local-dev workflows that set it. Added
  `_default_build_output_dir()`, used by both the duckdb client resolution and the default
  `output_folder`, so `dataset_files/` lands next to the duckdb file as expected.
- **`export()` crashed (`SameFileError`)** copying build artifacts when `output_folder` and
  `recipe_lock_path.parent` are the same directory — a direct consequence of the fix above, since
  that's now a legitimate, common case. Guarded with a same-path check.
- **The release zip swept in the raw multi-GB `.duckdb` file** and used an absolute path prefix
  for every entry, since `zip -r <zip> <output_folder>` was being run against a folder that (after
  the fix above) can now contain the multi-GB duckdb file. Now zips from within `output_folder`
  with relative paths and excludes `*.duckdb`.

## QA against a live Postgres build (`main` schema)

Compared row counts table-by-table against the real Postgres `main` schema build
(`db-green-fast-track`). That build stops at `int_spatial__all`/`int_flags__zoning` (fail-fast on
the same pre-existing LPC null-geometry issue tracked above), so `int_flags__spatial`,
`int_flags__all`, `green_fast_track_bbls`, and the `source__*` export models have nothing to
compare against on the Postgres side.

Every raw source table and every staging/intermediate table that exists on both sides matched
exactly, **except** `int_spatial__all` (317,192 postgres vs 317,429 duckdb, +237 = +79 × 3 across
`historic_resources`/`historic_resources_adj`/`shadow_hist_resources` — all three built from the
same `stg__lpc_landmarks` + `stg__nysshpo_historic_buildings` union, `LEFT JOIN`'d to `stg__pluto`
on `ST_WITHIN`). Diffing the underlying `(variable_id, bbl)` match pairs directly (not just row
counts) surfaced two distinct causes, one fixed above and one still open:

- The `usnname` NULL-vs-`''` divergence (fixed above) accounted for most of the *apparent*
  mismatch when diffing pairs naively (~2000 pairs looked mismatched purely because the label
  text differed) but turned out not to move the row count at all — expected, since it's a text
  difference, not a join-cardinality one.
- **DuckDB's `ST_WITHIN` produces a small number of fan-out matches PostGIS doesn't.** After the
  `usnname` fix, the real pair-level diff is 96 duckdb-only vs 133 postgres-only matches (net -37
  pairs), while `int_spatial__historic_resources` still nets +79 *rows* — consistent with a`LEFT
  JOIN`: postgres has zero landmarks matching more than one PLUTO lot (matched + unmatched sums
  exactly to the input row count), duckdb has ~79 net extra rows from a handful of landmark points
  matching two adjacent lots. Affects <0.1% of rows in the three historic-resources categories
  only. Likely a GEOS/precision difference between PostGIS and duckdb's spatial extension for
  points sitting on or very near a shared lot boundary — not chased further; see Open below.

## Fixed, but on `ar-fix-gft-nightly` (not yet in this branch's history)

- **153 LPC landmark records have `NULL` geometry** in the raw archive itself (historic
  districts, interior landmarks, multi-site designations — not a duckdb-specific issue, confirmed
  via a Postgres nightly GHA run hitting the identical failure). Filtered out in
  `stg__lpc_landmarks.sql`, with a `percent_not_null` guardrail test (fails if >1%). This branch
  will pick it up on merge/rebase; until then, `not_null_int_spatial__all_raw_geom`/
  `variable_geom` fail here as expected.

## GHA / devops (`.github/workflows/green_fast_track_build.yml`)

Rewrote the reusable build workflow to use the DuckDB pipeline, modeled on
`edde_build_category.yml`'s dcpy-native shape (`plan` → `load` → product build commands → upload)
rather than the old postgres `bash/build.sh` + `bash/export.sh` + legacy `dcpy.connectors.edm.publishing
upload` sequence. Kept the same `workflow_call` inputs (`image_tag`, `build_name`, `recipe_file`,
`plan_command`, `dev_bucket`) so `build.yml`/`repeat_build.yml` don't need changes.

- **`DUCKDB_PATH` can't be derived the same way `dcpy lifecycle builds build path --duckdb` does**
  for other products, because that CLI command reads the unresolved `recipe.yml` and requires a
  static top-level `version:`, but gft uses `version_strategy: pin_to_source_dataset`. Worked
  around with a small "Set DuckDB path" step that pulls the resolved `version` out of
  `recipe.lock.yml` (written by the preceding Plan step) via a one-line `python3 -c
  "import yaml; ..."` (chose this over `yq` since it's unconfirmed whether `yq` is installed in
  `nycplanning/build-base`, unlike `nycplanning/dev` where `bash/export_recipe_env.sh` already
  relies on it) and constructs `DUCKDB_PATH` directly, matching the `{product}_{version}.duckdb`
  filename convention `load.py`/`export.py` both already use internally.
- **The build's own DuckDB working file can't live where `BUILD_ENV_OUTPUT_DIR` naturally defaults
  to.** `_default_build_output_dir()`'s fallback (when the env var is unset) is
  `recipe_lock_path.parent` - i.e., the product source directory itself, since `recipe.lock.yml` is
  written there. That would put a multi-GB `.duckdb` file inside the git checkout. Set
  `BUILD_ENV_OUTPUT_DIR` explicitly to `${{ runner.temp }}/gft_build` (outside the checkout) at the
  job level instead, same as local dev's `.env`-based override.
- **`build upload <path>` uploads its entire argument directory with no filtering**
  (`BuildsConnector`/`s3.upload_folder`/`HybridPathedStorage.push` all copy everything they're
  given), and `export()` deliberately co-locates `dataset_files/`/`attachments/` next to the raw
  `.duckdb` working file in the same `BUILD_ENV_OUTPUT_DIR` (see the fix above). Pointing `upload`
  at the whole build dir would silently push the multi-GB duckdb file to `edm-publishing` on every
  build - something no other product does today. Instead, upload `dataset_files/` and
  `attachments/` as two separate calls, keeping the destination flat
  (`db-green-fast-track/build/<name>/{green_fast_track.gdb.zip,all_flags.csv,
  source_data_versions.csv,build_metadata.json}`) to match the existing bucket convention
  (verified live: `draft/`, `publish/`, and older `build/` folders all use this same flat layout).
- **That two-call upload would have clobbered itself**: `_BuildsConnector`/`HybridPathedStorage.push`
  `rmtree()`s the destination before copying, so a second `upload` call to the same
  `build/<name>/` key would delete the first call's files before adding its own. Added a `merge`
  parameter threaded end-to-end - CLI `--merge` flag → `upload_build()` → `_BuildsConnector
  .push_versioned()`'s `connector_args["merge"]` → `HybridPathedStorage.push()` →
  `LocalPathWrapper.copytree(merge=...)` (uses `dirs_exist_ok` for local paths; for S3, skips the
  `rmtree()` and lets `CloudPath.upload_from()` merge naturally) - so the first call
  (`dataset_files/`) replaces as usual and the second (`attachments/`, with `--merge`) adds to it
  instead of wiping it. Covered by `dcpy/test/connectors/test_hybrid_pathed_storage.py` (no prior
  test coverage existed for this connector at all).
- **`build_metadata.json` doesn't live in either uploaded subfolder** - `write_build_metadata()`
  puts it at the build root, alongside (not inside) `dataset_files/`/`attachments/`. It's required
  downstream by `dcpy.lifecycle.builds.artifacts.{builds,drafts,published}` (`get_build_metadata()`
  pulls it from exactly `{product}/build/{build}/build_metadata.json`), so silently dropping it
  would break draft/publish promotion. Copied it into `attachments/` right before upload so it
  rides along with that call rather than needing a third connector invocation.
- **`output.zip` and `recipe.lock.yml` are no longer uploaded** (previously present in the old
  postgres convention). Neither is read by any downstream dcpy code (checked
  `artifacts/{builds,drafts,published}.py`) - `output.zip` was a human-convenience bundle of the
  same content already in `dataset_files/`/`attachments/`. Accepted as a deliberate simplification
  rather than adding a third connector call or restructuring exports to recreate it.
- **`dcpy/test/lifecycle/builds/test_build.py` had 4 pre-existing failures** from this session's
  earlier `_write_gdb_zip`/`export_geodataset_from_duckdb` move into `dcpy/utils/duckdb.py` -
  3 tests still patched/imported the old `dcpy.lifecycle.builds.export` location, and one
  (`test_unsupported_format_from_duckdb_raises`) asserted shapefile export from duckdb was
  unimplemented, which stopped being true once `_write_shapefile_zip` was added. Fixed the patch
  targets and repointed the last test at a genuinely-unsupported format string instead of `shp`.
  Unrelated to today's GHA work but caught while validating it end-to-end.
- **Chose `DUCKDB_THREADS: 1` for the GHA runner default**, not `2`, specifically because of the
  known dbt-duckdb table-materialization race condition noted below under Open - a nightly job
  failing intermittently is worse than it running somewhat slower. `DUCKDB_MEMORY_LIMIT: 4GB` is a
  conservative starting point for the standard `ubuntu-22.04` runner (~7GB RAM/2 vCPU); unmeasured
  against a real gft build at full scale, may need tuning after the first real run.
- Verified locally end-to-end (not against real S3 - stopped short of pushing a test build to
  `edm-publishing`): recipe re-planned and re-exported cleanly after the `filename:
  green_fast_track.gdb.zip` and `stage_config.builds.build.{destination,destination_key,
  connector_args}` edits (`ogrinfo` confirms the zip is named correctly); `dcpy lifecycle builds
  build run` correctly executed the recipe's `dbt_build` command end-to-end (a real `dbt build`
  run), correctly raised on dbt's real exit code including surfacing the two pre-existing,
  already-documented failures above (LPC nulls, stale equality fixtures) - not new regressions.

## Open

- **`ST_WITHIN` fan-out mismatch between postgis and duckdb spatial** (see QA section above) — a
  handful of landmark points match two adjacent PLUTO lots in duckdb but only one in postgres,
  netting +79 rows across the three historic-resources-derived categories in `int_spatial__all`
  (<0.1% of ~90k rows each). Not chased to a fix; would need either accepting the discrepancy or
  forcing a consistent precision/snapping strategy on one side.
- **`equality_flags_zoning` / `equality_green_fast_track_pilot_projects` test fixtures are stale**
  against the currently-pinned PLUTO version (23-46 fixture BBLs don't exist in the current
  snapshot). Pre-existing, not duckdb-specific — would fail identically on Postgres. Needs
  fixture refresh or a version pin, not a query fix.
- **dbt-duckdb's table materialization has a race condition at `threads > 1`.** Its rename-swap
  (create as `__dbt_tmp`, rename old to `__dbt_backup`, rename tmp to final, drop backup) can be
  caught mid-swap by a concurrent reader, surfacing as `Catalog Error: Table with name
  X__dbt_backup does not exist!`. Reproduced multiple times at `threads: 2`. Worked around by
  running single-threaded; worth an issue against dbt-duckdb before relying on multi-threading for
  a real nightly job.
- **Memory tuning is machine-specific.** `profiles.yml` now reads `memory_limit`/`threads` from
  `DUCKDB_MEMORY_LIMIT`/`DUCKDB_THREADS` env vars (defaulting to `5GB`/`2`, today's values, when
  unset), so a GHA runner or other machine can set its own instead of editing the file. What
  values a GHA runner should actually use is still unmeasured — start from its available memory
  (see the `memory_limit`-below-80%-avoids-OOM note in the same file) and adjust from there.
  `preserve_insertion_order: false` is a general-purpose setting either way, not machine-specific.
- **`source_data_versions` isn't a real table in duckdb builds** the way it is for postgres
  (`load.py`'s `create_table_from_csv` call is gated on `has_postgres`), so it can't appear as a
  GDB layer the way the postgres export includes it. Currently omitted from `recipe.yml`'s
  `exports.datasets` for duckdb; it's still available as `attachments/source_data_versions.csv`
  either way via the automatic artifact copy, so this is a minor parity gap, not a missing
  deliverable.
- **`all_flags.csv` is ~600MB** (7.9M rows from `int_flags__all`). Not a bug, but worth a look if
  export time/size becomes a concern — e.g. parquet instead of csv, or trimming columns.
