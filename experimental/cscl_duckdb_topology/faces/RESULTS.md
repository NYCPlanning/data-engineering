# DuckDB face build: results

Can DuckDB spatial run the AtomicPolygon district face build (and the police overlay) that
`products/cscl` runs in PostGIS, and how fast? Starting point: PG's finished
`int__topology__edges`. The node/edge graph rebuild is in `../graph/`.

## What's here

| File | Purpose |
|---|---|
| `export.py` | PG -> Parquet in `data/` (gitignored): inputs plus PG's own outputs, for comparison |
| `sql/00_load.sql` | Parquet -> in-memory tables |
| `sql/10_classify_edges.sql` | `district_boundary_classify_edges` (land only) |
| `sql/20_build_raw.sql` | `district_boundary_build_raw`, split into faces / member_points / kept / union so each part is timed |
| `sql/30_strip_noise_rings.sql` | `district_boundary_strip_noise_rings` |
| `sql/40_validity.sql` | `district_boundary_validity` |
| `sql/police_*.sql` | `int__topology__ap_police`, materialized once per NYPD layer |
| `sql/90_compare.sql`, `sql/police_90_compare.sql` | correctness checks against PG's tables, run inside DuckDB |
| `run.py` | DuckDB runner: N reps, each in a fresh subprocess, every statement timed |
| `pg_bench.py` | PG timer: each dbt-compiled model as `CREATE TEMP TABLE ... AS`, one session, `statement_timeout = 15min` |
| `pg_compiled/` | `dbt compile` output of the PG models used for timing |

```bash
# from products/cscl, after `source load_direnv.sh`
python export.py --schema ar_cscl_districts_gdb_db_qa_oct4
python run.py --layers cb2010 nta2020 police --reps 3 --compare
python run.py --layers cb2010 nta2020 police --reps 3 --threads 1
python pg_bench.py --reps 3            # add --explain for one EXPLAIN ANALYZE per model
```

The finished tables live in `ar_cscl_districts_gdb_db_qa_oct4`, the parent branch's schema.
This branch's `BUILD_ENGINE_SCHEMA` (`ar_cscl_duckdb_topology_poc`) has no tables. Everything
here reads that schema and writes nothing to it. `pg_compiled/` was compiled with
`BUILD_ENGINE_SCHEMA` pointed at it. `dbt compile` doesn't run the `on-run-start` hook.

## Environment

- **DuckDB**: 1.5.5 with the spatial extension (`eb1e57c`), Python API. The machine is an
  Apple M1 laptop with 8 cores and 8 GB of RAM. DuckDB ran with 8 threads and a 6.3 GiB
  memory limit (the defaults). Peak RSS was about 1.3 GB.
- **Laptop load**: the 1-minute load average was about 47 to 55 throughout, because other
  work was running on the laptop at the same time. Sub-second DuckDB numbers are noisy (often
  ±50% between reps) and probably pessimistic.
- **PG**: PostgreSQL 18.6, PostGIS 3.6.4 and GEOS 3.14.1 on the remote build server. Settings:
  `max_parallel_workers_per_gather=2`, `max_parallel_workers=8`, `work_mem=7MB`,
  `shared_buffers=1585MB`, `jit=on`. Other users share the server, and its hardware is unknown.
- **Not apples to apples**: PG runs on a different, remote and shared machine. Read the ratios
  as orders of magnitude, not precise speedups. PG times are measured server-side
  (`CREATE TEMP TABLE AS`, so no result transfer). Each PG model reads its upstream inputs from
  the persisted, indexed tables, which helps PG. dbt's post-build index creation is not timed
  in either engine.

## Timings (seconds; median of 3; cold = rep 1)

On the DuckDB side, each rep runs in a new process with a new in-memory database. Cold and
warm runs barely differ in either engine, because the inputs are small and already in the
OS cache or buffer cache.

| Step | PG median (cold) | DuckDB 8 threads, median (cold) | DuckDB 1 thread, median | Rows | Matches PG? |
|---|---:|---:|---:|---:|---|
| cb2010 classify_edges | 9.54 (9.54) | 0.27 (0.36) | 0.85 | 533,951 | yes, identical row set |
| cb2010 build_raw, total | 8.22 (8.24) | 0.88 (0.99) | 2.03 | 38,797 | yes, ST_Equals 38,797/38,797 |
| · polygonize + dump (incl. edge scan) | ~5.3 † | 0.35 | 1.29 | 39,348 faces | |
| · member points (PointOnSurface) | ~0.2 † | 0.17 | 0.16 | 67,911 | |
| · kept (face contains member point) | ~1.1 † | 0.22 | 0.33 | 39,286 | |
| · union per entity | ~0.8 † | 0.14 | 0.25 | 38,797 | |
| cb2010 strip_noise_rings | 6.01 (5.74) | 0.11 (0.11) | 0.39 | 38,797 | yes, ST_Equals 38,797/38,797; symdiff area 0 |
| cb2010 validity | 6.96 (7.35) | 0.31 (0.27) | 1.16 | 38,797 | yes, all 7 flag columns identical; 38,797 is_valid in both |
| **cb2010 total** | **30.7** | **1.57** | **4.43** | | |
| nta2020 classify_edges | 8.31 (8.84) | 0.29 (0.29) | 0.71 | 368,659 | yes |
| nta2020 build_raw | 2.40 (2.15) | 2.93 (3.00) | 4.13 | 262 | yes, ST_Equals 262/262 |
| · of which kept | | 2.65 | 3.54 | 399 faces | |
| nta2020 strip_noise_rings | 3.04 (3.09) | 0.08 (0.08) | 0.36 | 262 | yes |
| nta2020 validity | 1.60 (1.60) | 0.09 (0.07) | 0.42 | 262 | yes |
| int__topology__ap_police | 33.17 (33.17) | 5.11 (5.09) | 7.81 | 69,786 | yes, 0 diffs on precinct / patrol borough / sector, globalids, method, shares |
| · aps (centroid, centroid_inside) | | 0.28 | 0.30 | | |
| · precinct | | 0.68 | 1.41 | | |
| · patrol borough | | 2.01 | 2.26 | | |
| · sector (beat) | | 2.07 | 3.75 | | |

† PG sub-steps come from a single `EXPLAIN ANALYZE` of the model (`data/results/pg_explain.txt`),
so they are approximate. PG runs the cb2010 polygonize as a serial `GroupAggregate` over an
index scan, and it is the bulk of build_raw.

**Not part of either benchmark:**
- Exporting PG to Parquet with the DuckDB `postgres` extension took about 35 s in total: edges
  8 s, APs 9 s, and the rest is PG reference outputs.
- Loading Parquet into DuckDB takes about 0.2 s.

## Correctness

The cb2010 and nta2020 results are identical to PG at every stage, and so is ap_police:

- **Edges**: every `(entity_id, node_lo, node_hi, is_arc, edge_type)` row matches, with
  `EXCEPT ALL` returning 0 in both directions.
- **Geometry** (`_raw` and final layers): entity ids match 1:1. Every geometry is
  `ST_IsValid` in both engines and `ST_Equals` to PG's. Part counts and `ST_NPoints` are the
  same. `ST_Area(ST_SymDifference)` is exactly 0. Area differs by at most 2e-6 sq ft, which is
  floating-point summation order. Hausdorff was not computed.
- **Validity**: each of the 7 per-entity validity columns is identical.
- **ap_police**: 0 differences on every column.

This isn't surprising: DuckDB spatial is also GEOS-backed, and the inputs are bit-identical
(Parquet carries the WKB).

## DuckDB gaps and workarounds

1. **Polygonize has a different shape.** In PostGIS, `ST_Polygonize` is an aggregate. In
   DuckDB spatial it is a scalar over `GEOMETRY[]`, so the edges go through `list()` first:
   `ST_Polygonize(list(geom)) ... GROUP BY entity_id`. DuckDB's `ST_Dump` returns
   `LIST(STRUCT(geom, path))` instead of a set-returning function, so it needs
   `unnest(..., recursive := true)`. Paths are 1-based like PostGIS's, and a single polygon
   still gets an empty path, so the macro's `coalesce(path[1], 1)` carries over.
2. **No `&&` operator, and no prepared geometry in a correlated EXISTS.** The macro's
   `EXISTS (... mp.entity_id = f.entity_id AND ST_Contains(f.face, mp.pt))` becomes a hash
   semi-join on `entity_id`, then a plain `ST_Contains` for each pair. That's fine for
   cb2010's small faces. For nta2020 it means 67k points tested against faces of up to 29k
   vertices, which costs 2.65 s and is the one step where DuckDB is slower than PG (PG: 2.40 s
   for all of build_raw). Rewriting it as a join on `ST_Contains`, which uses DuckDB's
   `SPATIAL_JOIN` operator, with the `entity_id` check as a filter brings it to about 1.6 s,
   with the same 399 kept faces. Adding `ST_Intersects_Extent` as a bbox prefilter made it
   slightly slower. The spatial-join rewrite was not adopted, so the SQL stays a literal port.
3. **Spatial joins work.** A bare `ST_Intersects` / `ST_Contains` join condition plans as
   `SPATIAL_JOIN` (an R-tree built on the fly, with no index DDL), which is what makes the
   police overlay fast. Joins on a spatial predicate plus an equi-key are planned on the
   equi-key (see 2).
4. **Spelling differences**, all mechanical:
   - `(array_agg(x))[1]` → `any_value(x)`
   - `(array_agg(x ORDER BY y DESC))[1]` → `first(x ORDER BY y DESC)`
   - `array_agg(... ORDER BY)` → `list(... ORDER BY)`
   - `CROSS JOIN LATERAL generate_series(...)` → `unnest(generate_series(...))`, because it
     returns a list in scalar position
   - `st_collect(geom)` as an aggregate → `ST_Collect(list(geom))`
   - `ST_GeometryType` returns `POLYGON` / `MULTIPOLYGON` (an enum, cast to VARCHAR) instead of
     `ST_Polygon`
   - `round(x::numeric, 4)` → `round(x, 4)` on a DOUBLE
   - `DISTINCT ON`, `FILTER`, `IS DISTINCT FROM`, and LEFT JOIN anti-patterns all work as-is.
     The port uses DuckDB's `ANTI JOIN` / `SEMI JOIN` for readability.
5. **Unions**: `ST_Union_Agg` gives the same result as PostGIS `ST_Union`. `ST_CoverageUnion_Agg`
   also exists and gives the same result here (ST_Equals on all of cb2010 and nta2020), but it
   wasn't faster: union is a small share of the time.
6. **Curves**: none needed handling. `int__topology__edges` is all LineStrings (arcs are
   densified upstream), `stg__atomicpolygons.geom` is already
   `st_makevalid(linearize(raw_geom))`, and the NYPD layers are MultiPolygons. `export.py`
   asserts that no input has arcs. DuckDB would hit the curve problem one stage earlier,
   wherever the raw curved AP geometry is consumed (`int__topology__vertices`).
7. **Postgres extension quirk**: `ATTACH` doesn't take a bound parameter for the DSN, so the
   string is inlined. Geometry is exported as `ST_AsBinary` through `postgres_query`, then
   rebuilt with `ST_GeomFromWKB`.
8. **Not ported**: `include_assigned_water=true`, which the `*wi` layers use. It's a small
   change to the `is_water` expression and the member-point filter.

## Verdict

DuckDB spatial handles the face build and the police overlay. Nothing failed, and the output
is geometrically identical to PG's (ST_Equals, symdiff 0, same validity) for all 38,797
census blocks, all 262 NTAs, and all 69,786 APs' police assignments.

On this laptop, under heavy competing load, DuckDB ran:
- the full cb2010 chain in about 1.6 s, against about 31 s on the shared PG server (about 20x;
  about 7x with DuckDB held to one thread);
- the police overlay in about 5 s, against about 33 s.

The machines differ, so treat these as "an order of magnitude", not a precise ratio. Most of
the gap is in relational and aggregation work (edge classification, validity, strip_noise),
where PG's plans are serial sorts and CTE scans, and in the polygonize aggregate (serial in PG,
parallel across entities in DuckDB). The one place DuckDB loses is point-in-face tests against
very large faces (nta2020's kept step), where PG is faster (likely PostGIS's prepared-geometry
caching) until the query is rewritten as a spatial join. The port itself was mechanical: spelling changes
only, no algorithmic changes.
