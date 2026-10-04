{{ config(
    materialized='table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

-- Every distinct vertex coordinate (arc mids excluded), grouped by exact equality - the
-- base point set before tolerance-based merging. Exact grouping can never split two
-- bit-identical points, unlike a bare ST_SnapToGrid, where two points closer than the
-- grid size can still straddle a cell boundary. Not partitioned by any district: a
-- vertex shared across a district border is one point.

SELECT
    -- deterministic ids, so each node's representative coordinate (int__topology__nodes)
    -- is the same on every build
    row_number() OVER (ORDER BY st_x(geom), st_y(geom)) AS point_id,
    geom,
    vertex_count
FROM (
    SELECT
        geom,
        count(*) AS vertex_count
    FROM {{ ref('int__topology__vertices') }}
    WHERE NOT is_arc_mid
    GROUP BY geom
) AS distinct_coords
