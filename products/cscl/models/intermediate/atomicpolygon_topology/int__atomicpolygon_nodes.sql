{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['grid_key']},
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

-- Canonical "topology nodes": every atomic-polygon ring vertex, grid-snapped to
-- {{ var('atomicpolygon_node_grid_size', 0.02) }}ft and collapsed to one node per grid
-- cell, centered on the true centroid of whatever vertices landed in that cell.
--
-- Deliberately a GROUP BY, not ST_ClusterDBSCAN: DBSCAN is a window function that has to
-- hold its whole partition in memory and run GEOS clustering internals over it in one
-- shot - not spatially partitionable, so at ~5M vertices citywide it's a real risk for
-- the crashes/timeouts already seen with it elsewhere. A grid-snap + GROUP BY is a plain
-- hash aggregate: deterministic, indexable, scales the boring way.
--
-- Tolerance picked from int__atomicpolygon_vertices' actual nearest-neighbor distances
-- (products/cscl chat log, 2026-09-20): ~37% of vertices already sit at literal
-- floating-point-identical coordinates with their nearest different-polygon neighbor; of
-- the rest, only ~3% fall between 0 and 0.03ft (the real "drift" band) before the
-- distribution steepens sharply past 0.03-0.05ft into what's almost certainly distinct,
-- unrelated vertices - not snapping candidates. 0.02ft sits inside that gap.
WITH grid_keys AS (
    SELECT
        atomicid,
        geom,
        ST_SnapToGrid(geom, {{ var('atomicpolygon_node_grid_size', 0.02) }}) AS grid_key
    FROM {{ ref('int__atomicpolygon_vertices') }}
)

SELECT
    row_number() OVER (ORDER BY grid_key) AS node_id,
    grid_key,
    st_centroid(st_collect(geom)) AS geom,
    count(*) AS vertex_count,
    count(DISTINCT atomicid) AS polygon_count
FROM grid_keys
GROUP BY grid_key
