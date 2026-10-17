# Porting CSCL models to DuckDB

Working notes for the Postgres to DuckDB cut-over (tracker epic inv-2cy). They cover the
patterns found so far, not the full list of constructs, which lives in
`duckdb_port_inventory.md`.

## Running

```bash
# load sources (about 1 minute, GDB downloads cached locally)
python -m dcpy lifecycle builds plan recipe --recipe-path recipe_duckdb.yml --output-path X.lock.yml
python -m dcpy lifecycle builds load load -r X.lock.yml
# build
dbt seed --target duckdb && dbt run --target duckdb -s <selector>
```

The `duckdb` target is in `profiles.yml`. `DUCKDB_PATH` must point at the file the load
wrote (`<build output dir>/cscl.duckdb`). Tables go in `BUILD_ENGINE_SCHEMA`.

The DuckDB versions of the plpgsql functions (`macros/duckdb_functions.sql`) are created
at run start on the duckdb target.

## Patterns

| Postgres | DuckDB | Note |
|---|---|---|
| `CROSS JOIN LATERAL st_dumppoints(g) AS d` (`d.path[n]`) | `unnest(dump_points(g)) AS d` in the **select list**; `d.part`, `d.ring`, `d.point` | Macro in `duckdb_functions.sql`. A lateral `CROSS JOIN unnest(...)` copies the whole input row (including large geometries) for every element and can run out of memory. |
| `CROSS JOIN LATERAL st_dump(g) AS d` | `unnest(st_dump(g)) AS d` in the select list; `d.geom`, `d.path` | `st_dump` is a scalar function returning a list of structs in DuckDB. |
| `st_geometrytype(g) = 'ST_Point'` | `= 'POINT'` (`'LINESTRING'`, `'POLYGON'`, `'MULTIPOLYGON'`, ...) | Upper case, no `ST_` prefix. |
| `st_curvetoline(CIRCULARSTRING ..., 0.00025, 1)` | `linearize_arc(x0, y0, xm, ym, x1, y1)` | DuckDB has no curve types. The macro is the load-time linearizer (`dcpy.geospatial.curves`): symmetric, with exact endpoints. It is not PostGIS's algorithm, so arc vertices move by at most 0.00025 ft. |
| `linearize(g)` on curve-loaded sources | drop it | Sources arrive already linear. Arcs are in `<table>_arcs`. |
| `generate_series(a, b) AS dx` (then `dx` as a value) | `generate_series(a, b) AS g (dx)` | Otherwise `dx` is the row (a struct). |
| `ARRAY[x]`, `ARRAY[]::geometry[]` | `[x]`, `[]::GEOMETRY[]` | |
| `arr \|\| elem` | `list_append(arr, elem)` | `\|\|` is list-to-list only. |
| `x = any(arr)` | `list_contains(arr, x)` | |
| `array_length(a, 1)` | `len(a)` | |
| `::double precision`, `::numeric` | `::DOUBLE`, `::DECIMAL` (or `::DOUBLE`) | |
| `st_setsrid(st_makepoint(x, y), srid)` | `st_point(x, y)` | No SRIDs here. |
| `a.geom && b.geom AND st_crosses(...)` | `st_crosses(...)` alone | DuckDB plans a spatial join on its own. |
| two CTEs each with `WINDOW w AS ...` | give them different names | "window w is already defined" |
| `x ~ 'pat'` | `regexp_matches(x, 'pat')` | **Silent bug:** in DuckDB, `~` is a FULL-string match. |
| `regexp_matches(s, re, 'g')` (set of matches) | `regexp_extract_all(s, re)` (a list) | Work on the list (`list_transform`, `array_to_string`) instead of `unnest` + `string_agg`, which has no defined order. |
| `col::INT,` with no alias | `col::INT AS col` | DuckDB names the column `CAST(col AS INTEGER)`. |
| `st_union(geom)` (aggregate) | `st_union_agg(geom)` | |
| `st_collect(geom)` (aggregate) | `st_collect(list(geom))` | |
| `st_polygonize(geom)` (aggregate) | `st_polygonize(list(geom))` | |
| `(st_dump(g)).geom` | `unnest(st_dump(g)).geom` in the select list | Aggregate in a subquery first if needed. |
| `rpad(s, n)` | `rpad(s, n, ' ')` | The fill character is required. |
| `ARRAY[]::geometry[]` with `array_agg` | `[]::GEOMETRY[]` with `list(...)` | |
| `round(float_expr)` | `round_even(float_expr, 0)` | **Silent:** Postgres `round(float8)` rounds ties to even (219710.5 becomes 219710); DuckDB `round` rounds ties away from zero. `round(numeric)` is away from zero in both. `double::INT` rounds ties to even in both. |
| `x::numeric` | `x::DECIMAL(38, 12)` | **Silent:** DuckDB's bare `NUMERIC` is `DECIMAL(18,3)`, so it rounds to 3 places first. |
| `to_char(d, 'MMDDYYYY')` | `strftime(d, '%m%d%Y')` | |
| `g1 @ g2` (bbox contained by) | compare `st_xmin`, `st_xmax`, `st_ymin`, `st_ymax` | |
| `st_setsrid(st_extent(g)::geometry, ...)` (aggregate) | `st_extent_agg(g)` | |
| `st_hausdorffdistance(a, b)` | `hausdorff_distance(a, b)` | Macro (GEOS discrete Hausdorff). |
| `LATERAL fn(...) AS o (a geometry, b geometry)` (record) | `fn(...) AS o` in a subquery's select list, then `o.a`, `o.b` | `offset_points` returns a struct. Macros must be plain expressions: one whose subqueries reference its arguments can't be called on table columns. |
| `x = any(arr)` | `list_contains(arr, x)` | |
| `format('... %s ...', x)` | `format('... {} ...', coalesce(x::VARCHAR, ''))` | **Silent:** DuckDB's `format` uses `{}` and leaves `%s` as literal text; a NULL argument makes the whole result NULL, where Postgres renders it as ''. |
| `indexes=[...]` config | leave it | Skipped on DuckDB by `macros/duckdb_adapter_overrides.sql`. |

## Performance traps

- **LEFT JOIN with a predicate on only one side** (`ON a.k = b.k AND NOT a.flag`) makes
  DuckDB give up the hash join and nested-loop. It took `edge_sides` from 0.4 s to 229 s.
  Fold the predicate into the key: `ON (CASE WHEN a.flag THEN NULL ELSE a.k END) = b.k`.
  Or drop it if the select list already handles it.
- Same trap, other form: `LEFT JOIN t ON a.id = t.id AND a.feature_type = 'centerline'`
  (`int__lion` went from more than 9 minutes to 2 s). Fold it:
  `ON (CASE WHEN a.feature_type = 'centerline' THEN a.id END) = t.id`.
- **A spatial predicate in the same join as cheap equality keys** can produce a bad plan.
  Join on the keys in a `MATERIALIZED` CTE, then filter with the spatial predicate.
- **A lateral `unnest`** of large lists: see the first pattern above.
- Joins on GEOMETRY equality hash fine.
- **Repeated `st_contains` against one large polygon** is slow: DuckDB doesn't cache
  prepared geometries the way PostGIS does. Turn it around and spatial-join small things
  to a point. For example, `district_boundary_build_raw` looks up the AP under each
  face's `st_pointonsurface` (hez_raw went from 182 s to 1.3 s).
- **Memory:** GEOS allocates outside DuckDB's `memory_limit`. The profile caps DuckDB at
  3GB and 2 threads (`DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS`), because the default 80% of
  RAM ran an 8 GB machine out of memory.

## Validating against the Postgres build

Attach the Postgres build from DuckDB and compare with `EXCEPT ALL` (`load postgres;
attach '<BUILD_ENGINE_SERVER>/db-cscl' AS pg (TYPE postgres, READ_ONLY)`). Geometry comes
back as EWKT text: `st_geomfromtext(regexp_replace(geom::varchar, '^SRID=[0-9]+;', ''))`.

Compare by geometry and attributes, not generated IDs. Row-number IDs over sorted
coordinates shift because of a source-loading difference that is not a porting issue:
the 2 bezier AtomicPolygons near (913610, 124810) linearize with about 1e-10 ft float
noise that differs between GDAL versions.
