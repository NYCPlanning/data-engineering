# DuckDB port inventory

Postgres/PostGIS-specific constructs in `models/` and `macros/` (excluding `create_pg_functions.sql`).
Counts are files containing the construct (grep, case-insensitive); they are approximate, and
include QA-only models (`etl_dev_qa`), which need not be ported.

| construct | files | example | DuckDB equivalent |
|---|---|---|---|
| `st_*` functions (~40 distinct; top: st_area, st_perimeter, st_x/y, st_multi, st_transform, st_difference, st_dump) | 89 | `staging/stg__censustract2000.sql` | Mostly direct via spatial. Gaps below. |
| `st_hausdorffdistance` | 4 | `macros/geometry_diff_qa.sql` | needs port (no DuckDB function); QA only |
| `st_curvetoline` | 3 | `intermediate/atomicpolygon_topology/int__topology__edges_unnoded.sql` | replaced by load-time linearization (`dcpy.geospatial.curves`) |
| `st_dumpsegments` | 6 | `etl_dev_qa/qa__context_geoms.sql` | needs port (list_transform over `st_dump(st_points(line))`, as in `offset_points` macro) |
| `st_setsrid` / `st_srid` | ~8 | `intermediate/other_lion_intermediates/int__centerline_curve.sql` | no SRID on DuckDB geometry; `st_setcrs`/`st_crs` or track SRID by convention |
| `st_transform` | 26 | `macros/geometry_diff_qa.sql` | exists; needs explicit source CRS (`always_xy`) |
| `st_makevalid`, `st_union`, `st_buffer`, `st_pointonsurface`, `st_linemerge`, `st_collectionextract` | 3-13 each | `macros/district_boundary.sql` | exist; GEOS results can differ at 1e-9 level or in vertex order |
| `<->` KNN distance | 0 in models (only in `offset_points`) | n/a | `st_distance` |
| `DISTINCT ON` | 16 | `etl_dev_qa/district_boundaries/qa__topology__arc_disagreements.sql` | needs port: `QUALIFY row_number() OVER (...) = 1` (DuckDB also has `arg_min`) |
| `LATERAL` | 17 | `etl_dev_qa/qa__test_cases_geometry.sql`; `int__segment_offsets.sql`, `int__segment_topology_sides.sql` (offset_points) | supported; record-returning `LATERAL f() AS o (a geometry, b geometry)` needs port to struct field access |
| `generate_series` | 7 | `product/lion/gdb/gdb_lion.sql` | supported (`generate_series`/`range`) |
| jsonb ops (`->>`, `#>>`, `jsonb_*`) | 42 | `etl_dev_qa/qa__lion_dat_individual_diffs.sql` | mostly QA only; `->>` works on JSON type, `jsonb_*` names need port |
| regex ops (`~`, `!~`) | 6 | `staging/stg__facecode_and_featurename.sql` | needs port: `regexp_matches(x, pat)` |
| `regexp_replace`/`regexp_matches`/`regexp_split` | 4 | same | supported (flags syntax differs: `'g'` option) |
| `string_agg` | 1 | `staging/stg__facecode_and_featurename.sql` | supported |
| `array_agg` | 33 | `etl_dev_qa/diffs/altnames/qa__diffs_fgdb_altnames.sql` | supported (`list`); `(array_agg(x))[1]` works (1-based) |
| `unnest` | 3 | `staging/stg__facecode_and_featurename.sql` | supported |
| array functions / `ANY (...)` / `ARRAY[...]` | 43 | `etl_dev_qa/qa__lion_dat_by_row.sql` | `= ANY(list)` supported, `array_length(x,1)` needs port (`len`) |
| `::geometry`, `::regclass`, `::text[]`, `::int[]` casts | 12 | `etl_dev_qa/district_boundaries/qa__boundary__cb2010_212_pair.sql` | `::geometry` supported; `::regclass` needs port |
| scalar `::` casts (numeric, int, text, varchar, ...) | 106 | `staging/_stg.yml` | supported; `numeric` is DECIMAL (watch default precision); `::text` fine |
| `lpad` / `rpad` | 29 | `staging/stg__altsegmentdata_proto.sql` | supported, same truncation semantics |
| `to_char` | 1 | `intermediate/ldf/int__ldf_header.sql` | needs port: `strftime` / `printf` |
| `format()` | 8 | `log_files/log__lion_segment_lgc_count.sql` | `format()` supported (fmt-style `{}`), `%s` needs port to `printf` |
| `FILTER (WHERE ...)` | 8 | `etl_dev_qa/qa__lion_dat_summary.sql` | supported |
| `chr`, `concat_ws`, `split_part`, `md5`, window functions | 1/9/6/4/24 | `product/lion/gdb/gdb_altnames_by_field.sql` | supported |
| dbt adapter/`dbt_utils`/`run_query` | 58 | `models/sources.yml` | dbt-duckdb adapter: sources and Postgres-only config (indexes) need port |

## plpgsql functions (ported in `macros/duckdb_functions.sql`)

| function | call sites (files) | DuckDB |
|---|---|---|
| `format_lion_text` | 0 in models/macros | macro; parity tested |
| `convert_level_code` | 1 (`intermediate/int__lion.sql`) | macro; parity tested |
| `find_circle` | 1 (`other_lion_intermediates/int__centerline_curve.sql`) | macro returning `POINT Z` (x, y, radius); parity tested on real curved centerlines |
| `offset_points` | 2 (`int__segment_offsets.sql`, `int__segment_topology_sides.sql`) | macro returning `STRUCT(left_offset_point, right_offset_point)`; parity tested on real centerlines. Caller needs struct access instead of `LATERAL ... AS o (l geometry, r geometry)`. Takes the first matching segment when more than two match (Postgres raises). |
| `linearize` | see below | replaced by load-time linearization; no-op on already-linear geometry. Stand-in macro keeps the `ST_LineMerge` branch for MultiLineString. |
| `test_cases.geometry_diff_metrics` | 2 (QA: `geometry_diff_qa.sql`, `qa__test_cases_geometry.sql`) | not ported (lowest priority). Gap: `st_hausdorffdistance` has no DuckDB function. The rest (`st_symdifference`, `st_simplify`, `st_dump`, ring functions, `st_perimeter`) exists; workaround is a table macro with the Hausdorff metric left NULL, or a Python UDF using shapely. |

### `linearize()` call sites

All on source geometry (curved on load), i.e. covered by load-time linearization:
`staging/stg__atomicpolygons`, `stg__borough`, `stg__businessimprovementdistrict`, `stg__censustract2000`,
`stg__electiondistrict`, `stg__firecompany`, `stg__historicdistrict`, `stg__urbanrenewalarea`, `stg__zipcode`;
`macros/district_boundary.sql` (prod shape, 2 sites); `macros/geometry_diff_qa.sql` (build and prod geometry, 8 sites); `etl_dev_qa/district_boundaries/qa__boundary__cb2010_212_pair.sql`.

`intermediate/atomicpolygon_topology/int__topology__edges_unnoded.sql` only mentions it in a comment
(it inlines `ST_CurveToLine` with the same parameters).

Flag: the `geometry_diff_qa.sql` call sites linearize `build_geom_column`, which is a build-side column.
That is fine if the column derives from linearized sources, but it is the one place geometry could
be curved mid-build (e.g. prod FGDB geometry loaded raw). Check how prod geometry is loaded in DuckDB.
