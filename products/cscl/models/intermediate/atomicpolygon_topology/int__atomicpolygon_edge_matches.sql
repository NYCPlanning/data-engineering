{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['node_lo', 'node_hi']},
      {'columns': ['is_resolved']},
    ]
) }}

-- Resolves orphan edges (from int__atomicpolygon_shoreline_edges) against the nearest
-- edge belonging to a genuinely different atomicid, of ANY edge_type - including other
-- orphans. Excluding other orphans from this search was the bug behind the East River
-- false positives (see chat log, 2026-09-20): two adjacent water pieces that split a
-- river don't node-match each other, so each side's edge is "orphan," but they sit right
-- on top of each other - the true nearest neighbor for an orphan edge is very often
-- another orphan, not a "confirmed match" edge_type.
--
-- is_resolved (dist < {{ var('atomicpolygon_edge_match_tolerance', 5) }}ft) means: this
-- edge is provably not a real gap - something else (of any kind) sits essentially on top
-- of it, and the "orphan" label is just a vertex-bookkeeping artifact (mechanism
-- B/C - vertex-density mismatch or a cross-atomicid seam), not a coverage hole.
-- Unresolved edges are the genuine candidates worth investigating.
--
-- This does NOT yet insert the host's missing vertex into the other ring - it only
-- classifies. Turning a resolved match into an actual shared node (so graph traversal /
-- ST_LineMerge can walk through it) is a further step: project this edge's endpoints
-- onto the host edge and splice them into the host ring at the right position.
WITH orphans AS (
    SELECT node_lo, node_hi, atomicids, edge_type, geom
    FROM {{ ref('int__atomicpolygon_shoreline_edges') }}
    WHERE edge_type IN ('orphan_water_edge', 'orphan_land_edge')
)

SELECT
    o.node_lo,
    o.node_hi,
    o.atomicids,
    o.edge_type,
    nn.node_lo AS host_node_lo,
    nn.node_hi AS host_node_hi,
    nn.atomicids AS host_atomicids,
    nn.edge_type AS host_edge_type,
    nn.dist,
    nn.dist < {{ var('atomicpolygon_edge_match_tolerance', 5) }} AS is_resolved
FROM orphans AS o
CROSS JOIN LATERAL (
    SELECT e.node_lo, e.node_hi, e.atomicids, e.edge_type, e.geom <-> o.geom AS dist
    FROM {{ ref('int__atomicpolygon_shoreline_edges') }} AS e
    WHERE NOT (e.atomicids && o.atomicids)
    ORDER BY e.geom <-> o.geom
    LIMIT 1
) AS nn
