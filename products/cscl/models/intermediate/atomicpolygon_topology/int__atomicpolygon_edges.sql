{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['node_lo', 'node_hi']},
      {'columns': ['atomicid']},
    ]
) }}

-- Every ring edge (consecutive node pair) of every atomic polygon, keyed so the same
-- physical edge traced by two adjacent polygons (in opposite winding order) lands on the
-- same (node_lo, node_hi) key regardless of direction. This is the arc-node "edge" layer:
-- classifying shoreline becomes a GROUP BY on this table (see
-- int__atomicpolygon_shoreline_edges) instead of a polygon overlay (ST_Union/ST_Difference),
-- which is what was producing hundreds of thousands of interior-ring holes on a plain
-- land-minus-water dissolve (see products/cscl chat log, 2026-09-20).
--
-- Rings are already closed by ST_DumpPoints (first point == last point), so LEAD() ordered
-- by point_ordinal naturally produces exactly ring-length-minus-one edges with no
-- wraparound case to special-case - the trailing NULL row (nothing after the closing point)
-- is dropped.
WITH edges AS (
    SELECT
        atomicid,
        part_index,
        ring_index,
        point_ordinal,
        node_id AS node_a,
        lead(node_id) OVER (
            PARTITION BY atomicid, part_index, ring_index ORDER BY point_ordinal
        ) AS node_b
    FROM {{ ref('int__atomicpolygon_vertices_snapped') }}
)

SELECT
    atomicid,
    part_index,
    ring_index,
    point_ordinal,
    node_a,
    node_b,
    least(node_a, node_b) AS node_lo,
    greatest(node_a, node_b) AS node_hi
FROM edges
WHERE node_b IS NOT NULL
