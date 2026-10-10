# AP topology node/edge graph: DuckDB vs Postgres

POC for tracker bead inv-oh1 (graph half). The goal is to validate and benchmark, not to port.
The face/polygon build is in `../faces/`.

Scope: `int__topology__exact_points` → `grid_cells` → `point_to_node` → `nodes` →
`edges_unnoded` → `edge_crossings` → `edges` (products/cscl/models/intermediate/atomicpolygon_topology),
rebuilt in DuckDB from PG's `int__topology__vertices`. The data is citywide: 816,499 vertices,
including 16,202 distinct arcs.

## How to run

```bash
cd products/cscl && source load_direnv.sh && cd ../../experimental/cscl_duckdb_topology/graph
python export_from_pg.py --schema ar_cscl_districts_gdb_db_qa_oct4  # PG -> data/*.parquet
python run.py                     # DuckDB build, 3 runs; --threads 1 for single-threaded
python compare.py                 # DuckDB vs PG, row- and key-level
python arc_check.py               # arcs rebuilt in DuckDB vs PG's linearized arcs
(cd ../../../products/cscl && dbt compile --select int__topology__exact_points \
  int__topology__grid_cells int__topology__point_to_node int__topology__nodes \
  int__topology__edges_unnoded int__topology__edge_crossings int__topology__edges)
python pg_bench.py --source-schema ar_cscl_districts_gdb_db_qa_oct4  # PG, temp tables only
python stress.py 500 1000 2000    # synthetic recursion stress test
```

Source schema: the topology tables exist only in `ar_cscl_districts_gdb_db_qa_oct4` (the parent
branch's build). This branch's schema (`ar_cscl_duckdb_topology_poc`) is empty. The model SQL is
the same on both branches. `pg_bench.py` writes nothing outside session temp tables.

Export (not part of the benchmark): it took 45 s wall time for everything, through DuckDB's
`postgres` extension (`postgres_query` + `ST_X`/`ST_Y`/`ST_AsBinary`) to Parquet. Of that,
vertices took 7 s and the arc lines 6 s. The rest is PG's reference outputs, which are used only
for comparison.

## Timings

| step | PG CTAS (+ index/analyze) | DuckDB, 8 threads | DuckDB, 1 thread | rows | matches PG? |
|---|---|---|---|---|---|
| exact_points | 1.89 s (+1.40) | 0.05 s | 0.07 s | 296,012 | yes, exact |
| grid_cells | 0.97 s (+1.69) | 0.06 s | 0.11 s | 292,248 | yes, exact |
| point_to_node (UNION closure) | 5.32 s (+0.14) | 0.08 s | 0.15 s | 296,012 | yes, exact |
| point_to_node (USING KEY, DuckDB only) | n/a | 0.08 s | 0.14 s | 296,012 | yes (same as closure) |
| nodes | 1.14 s (+1.55) | 0.05 s | 0.07 s | 288,199 | yes, exact |
| edges_unnoded | 39.36 s (+7.09) | 0.59 s (0.32 raw_edges + 0.27 rest) | 1.21 s | 714,588 | yes, exact incl. geometry |
| edge_crossings | 2.05 s (+0.02) | 0.09 s | 0.23 s | 22 | yes, exact |
| edges | 1.87 s (+4.31) | 0.14 s | 0.20 s | 714,615 | yes, exact incl. geometry |
| **total** | **52.6 s (+16.2)** | **1.06 s** | **2.04 s** | | |

All times are medians of 3 runs. Cold and warm runs barely differ in either engine:

- DuckDB's cold run (a fresh process) is within ±0.04 s per step of its median, because all
  data is in memory once loaded. Loading the Parquet takes 0.07 s. A fresh process doing
  everything, including loading `spatial`, reading Parquet and writing `graph.duckdb`, takes
  2.7 s wall time.
- PG's first run is 0–15% slower per step. PG reads `int__topology__vertices` from the source
  schema and every later step from session temp tables. The model's dbt indexes are built on
  each temp table, followed by `ANALYZE` (temp tables are never auto-analyzed). That work is
  shown separately in parentheses.

**This is not an apples-to-apples comparison.**

- **DuckDB** 1.5.5 (Python) ran on this laptop: an Apple M1 with 8 cores and 8 GB RAM, using
  8 threads (or 1) and the default `memory_limit` of 6.3 GiB.
- **PG** 18.6 with PostGIS 3.6.4 / GEOS 3.14.1 is a remote shared server. Its settings:
  `max_parallel_workers_per_gather=2`, `max_parallel_workers=8`, `work_mem=7MB`,
  `shared_buffers=1585MB`, `jit=on`. 3 other sessions were active at the start. Its hardware
  is unknown.

The DuckDB 1-thread column is the closest like-for-like comparison available.

## Correctness

`compare.py` checks every table two ways:

- **By key, exactly.** Node and point ids are deterministic (row_number over coordinates) in
  both engines, so they line up.
- **Independently of ids.** It compares point clusters as sets of member coordinates, nodes as
  coordinate pairs, and edges as an unordered endpoint coordinate pair + `is_arc` + `atomicid`.

Geometry is compared byte-for-byte as WKB.

**Result: every table matches PG exactly, with zero differing rows, geometry included.** This
covers all 422 orphan-edge splits and all 22 crossing rows.

The match on arcs is partly by construction, because the arc vertex lists are carried over from
PG (see below). `arc_check.py` covers that gap.

## Gaps and workarounds

1. **DuckDB has no curve types** (no `CIRCULARSTRING`, no `ST_CurveToLine`).
   - **Workaround used:** the build carries PG's linearized arc for each arc key
     (`arc_lines.parquet`), keyed by its exact endpoint coordinates. DuckDB still computes the
     arc keys `(node_lo, node_hi, is_arc)` itself, including the lowest-atomicid canonical
     owner. Only the vertex list comes from PG.
   - **Alternative, validated but not wired in:** `sql/arc_reconstruct.sql` rebuilds every arc
     from its 3 control points in plain SQL. It ports liblwgeom's `lw_arc_center` and
     `lwarc_linearize` (max-deviation 0.00025 ft) and takes 0.34 s.
   - Of the 16,202 arcs, 15,900 are bit-identical to PG's. All 16,202 have the same number of
     points, and the largest vertex deviation is 4.7e-10 ft, which is 1 ulp at state-plane
     magnitudes. The difference comes from libm `sin`/`cos` and from computing `a1 + k*inc`
     instead of a running sum.
   - Reconstruction is viable if DuckDB ever has to stand on its own. Ends are snapped to node
     coordinates and every owner shares one canonical curve, so topology is unaffected.
2. **Planner trap 1: computed join keys.** PG's 9-neighbour join,
   `ON b.x_cell = a.x_cell + dx AND b.y_cell = a.y_cell + dy` with `dx`/`dy` from a cross-joined
   `generate_series`, is planned by DuckDB as a NESTED_LOOP_JOIN on `a.cell_id != b.cell_id`
   over 292k × 292k × 3 rows. It never finished (killed after more than 10 min).
   - **Fix:** compute `nx`/`ny` in their own CTE, then join on plain column equality, which
     gives a HASH_JOIN that takes 0.03 s.
3. **Planner trap 2: one-sided predicates in a LEFT JOIN's ON.** For example,
   `LEFT JOIN exact_points ON p.x = ec.x AND p.y = ec.y AND NOT p.is_arc_mid`. DuckDB turns this
   into a BLOCKWISE_NL_JOIN that ran for more than 1 min instead of 0.3 s.
   - **Fix:** fold the predicate into the key: `(CASE WHEN NOT p.is_arc_mid THEN p.x END) = ec.x`.
   - The same fix was applied twice in `05b_edges_unnoded.sql`.
   - Both of these traps are silent: the query just runs forever. They should be added to
     `admin/ops/pg_to_duckdb_sql.py`.
4. **Rounding.** PG's `round(double)` rounds half to even (`rint`). DuckDB's `round()` rounds
   half away from zero.
   - 47,084 of the 296k points sit exactly on a half-cell for this grid size. The tolerance is
     3/8192 ft, a binary fraction, so this happens often.
   - With plain `round()`, `grid_cells` would differ from PG. `round_even(x, 0)` is the faithful
     spelling.
5. **Spatial join and R-tree.** `a.geom && b.geom AND ST_Crosses(...)` is planned as DuckDB's
   SPATIAL_JOIN, which builds an R-tree on the fly; no persistent index is needed.
   `ST_Intersects`, `ST_Crosses` alone and `ST_Intersects_Extent` produce the same plan. The
   timed spellings (`&&`, `ST_Intersects`, `ST_Crosses`) each take about 0.1 s for about 2.3k
   one-owner probes against 680k edges. The 1-thread `edge_crossings` figure in the table was
   measured with the `ST_Intersects` spelling.
6. **Recursive CTEs.**
   - On the real data, both forms are trivial. 8,114 cell merges give 16,204 reachable pairs in
     2 iterations, and the closure and USING KEY variants agree exactly.
   - DuckDB's `path_search` (2,288 orphans, 270k path rows, 6 hops, carrying `GEOMETRY[]` lists)
     works as written, using `list_append` and `list_contains`.
   - DuckDB 1.5 warns that `UNION` in a `USING KEY` CTE is deprecated. Use `UNION ALL`, and
     reference the accumulated table as `recurring.<cte>`.
   - `stress.py` builds one chain-shaped component of n cells, the worst case:

     | n | DuckDB closure (UNION) | DuckDB USING KEY | PG closure (UNION) |
     |---|---|---|---|
     | 500 | 0.10 s | 0.30 s | 1.25 s |
     | 1000 | 0.30 s | 0.52 s | 4.55 s |
     | 2000 | 1.03 s | 1.44 s | 14.08 s |
     | 4000 | 3.73 s | 3.40 s | skipped |
     | 8000 | 23.08 s | 11.70 s | skipped |

     The closure is O(n²) rows in both engines. USING KEY label propagation stores n rows and
     overtakes the closure at around n = 4000. Real clusters are 2–3 cells, so this only
     matters if the merge tolerance or the data changes.
7. **Minor differences.**
   - `by` is a reserved word in DuckDB, so it can't be used as a column alias.
   - `ST_Dump` returns a list of structs (use `unnest(...).geom`).
   - `ST_GeometryType` returns `'POINT'` rather than `'ST_Point'`.
   - `DISTINCT ON` and row-value comparison `(a, b) < (c, d)` both work in DuckDB. The
     `distinct-on` rule in `admin/ops/pg_to_duckdb_sql.py`, which says DuckDB doesn't support
     it, is wrong.
   - There is no `ST_HausdorffDistance`; `arc_check.py` uses a vertex-to-line maximum instead.

## Where PG's time goes (edges_unnoded, EXPLAIN ANALYZE, 42.7 s)

Row estimates are off by up to 10¹³ (e.g. `rows=1.2e19` on the recursive union). That causes
three things:

- **JIT, 2.9 s.** The bad estimates trigger JIT compilation.
- **Re-sorting in the recursion, ~17 s.** The path_search recursion is a Merge Join that
  re-sorts the 1.36M directed edges on each of its 6 iterations, about 2.5 s each.
- **Arc linearization, ~5.3 s.** This is the `format` → `ST_GeomFromText` → `ST_CurveToLine`
  pass over 32k arc rows.

For `point_to_node`, the recursion itself takes 35 ms in PG. The 5 s is the 2.6M-row neighbour
join plus `ST_DWithin` on serialized geometry.

So PG's 40 s is largely a plan problem, not inherent cost. Turning off `jit` and materializing
`directed_edges` as an indexed table would probably close much of the gap. That was not tried
here.

## Verdict

DuckDB reproduces the whole node/edge graph exactly: every row, key and geometry. Each of PG's
idioms has a direct DuckDB spelling, including the recursive CTEs with list state, the
`DISTINCT ON` steps and the spatial-join noding.

Two blockers are real but small:

- **Curves.** Solved by carrying PG's arcs, or by the validated 3-point reconstruction.
- **Two silent planner traps** (computed join keys, and one-sided LEFT JOIN predicates). Each
  turns a sub-second query into one that never finishes. Anyone porting needs to know them.

On this laptop, DuckDB builds the graph in about 1 s (2 s single-threaded), against about 53 s
of CTAS (69 s with indexes) on the shared PG server. That speedup is real but overstated: most
of PG's time is one badly estimated query, and the machines differ.

At citywide scale (816k vertices), both engines are fast enough. These results support DuckDB
as a plausible home for this stage, but they don't by themselves show that moving is necessary.
