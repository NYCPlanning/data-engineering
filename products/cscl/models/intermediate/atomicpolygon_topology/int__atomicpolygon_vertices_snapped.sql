{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['atomicid']},
      {'columns': ['node_id']},
    ]
) }}

-- Each original ring vertex resolved to its canonical topology node (int__atomicpolygon_nodes).
-- This is the join that lets a ring/polygon be rebuilt from shared nodes instead of literal
-- per-polygon coordinates - two atomic polygons whose boundary vertices snapped to the same
-- node are now structurally guaranteed to share that exact coordinate, not just close to it.
WITH vertices AS (
    SELECT
        atomicid,
        part_index,
        ring_index,
        point_ordinal,
        geom AS original_geom,
        ST_SnapToGrid(geom, {{ var('atomicpolygon_node_grid_size', 0.02) }}) AS grid_key
    FROM {{ ref('int__atomicpolygon_vertices') }}
)

SELECT
    v.atomicid,
    v.part_index,
    v.ring_index,
    v.point_ordinal,
    v.original_geom,
    n.node_id,
    n.geom AS node_geom
FROM vertices AS v
INNER JOIN {{ ref('int__atomicpolygon_nodes') }} AS n ON v.grid_key = n.grid_key
