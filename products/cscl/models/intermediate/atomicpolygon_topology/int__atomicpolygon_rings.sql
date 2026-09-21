{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['atomicid']},
    ]
) }}

-- Rebuilds each ring (ring_index 1 = exterior, 2+ = interior/holes) as a closed
-- linestring made of shared node coordinates instead of each polygon's own original
-- vertices. Two atomic polygons that snapped to the same node at their shared boundary
-- now produce rings that pass through the exact same coordinate there, not just a nearby
-- one. Ring closure (first point == last point) falls out for free: ST_DumpPoints always
-- emits the same original coordinate for a ring's first and last point, so they snap to
-- the same node deterministically.
SELECT
    atomicid,
    part_index,
    ring_index,
    st_makeline(array_agg(node_geom ORDER BY point_ordinal)) AS ring_geom
FROM {{ ref('int__atomicpolygon_vertices_snapped') }}
GROUP BY atomicid, part_index, ring_index
