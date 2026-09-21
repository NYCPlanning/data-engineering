{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['node_lo', 'node_hi']},
      {'columns': ['edge_type']},
    ]
) }}

-- Classifies every unique edge (undirected node pair) by who owns it and on which side of
-- water_flag. No polygon overlay - purely a GROUP BY over int__atomicpolygon_edges joined
-- to water_flag, so it's cheap regardless of how big the underlying polygons are.
--
--   shoreline         - exactly 2 owning atomicids, one water_flag='1' and one not: this
--                        IS the coastline, read directly off the topology.
--   internal_seam     - exactly 2 owners, same side of water_flag: an ordinary interior
--                        boundary between two land parcels or two water polygons.
--   orphan_water_edge - exactly 1 owner, water side: either a true open edge (coastline
--                        against nothing further, or the dataset boundary) or the
--                        vertex-density-mismatch case where the land side has no vertex
--                        at this node at all - the real "leftover water" candidates.
--   orphan_land_edge  - exactly 1 owner, land side: same idea, land-side orphan.
--   multi_owner       - more than 2 owners; unexpected for planar polygons, worth
--                        investigating on its own if it shows up at any real volume.
WITH edges AS (
    SELECT
        e.node_lo,
        e.node_hi,
        e.atomicid,
        a.water_flag
    FROM {{ ref('int__atomicpolygon_edges') }} AS e
    INNER JOIN {{ ref('stg__atomicpolygons') }} AS a ON a.atomicid = e.atomicid
),

classified AS (
    SELECT
        node_lo,
        node_hi,
        array_agg(DISTINCT atomicid) AS atomicids,
        array_agg(DISTINCT water_flag) AS water_flags,
        count(DISTINCT atomicid) AS owner_count,
        bool_or(water_flag = '1') AS has_water_owner,
        bool_or(water_flag <> '1') AS has_land_owner
    FROM edges
    GROUP BY node_lo, node_hi
)

SELECT
    c.node_lo,
    c.node_hi,
    c.atomicids,
    c.water_flags,
    c.owner_count,
    CASE
        WHEN c.owner_count = 2 AND c.has_water_owner AND c.has_land_owner THEN 'shoreline'
        WHEN c.owner_count = 2 THEN 'internal_seam'
        WHEN c.owner_count = 1 AND c.has_water_owner THEN 'orphan_water_edge'
        WHEN c.owner_count = 1 THEN 'orphan_land_edge'
        ELSE 'multi_owner'
    END AS edge_type,
    st_makeline(n_lo.geom, n_hi.geom) AS geom
FROM classified AS c
INNER JOIN {{ ref('int__atomicpolygon_nodes') }} AS n_lo ON n_lo.node_id = c.node_lo
INNER JOIN {{ ref('int__atomicpolygon_nodes') }} AS n_hi ON n_hi.node_id = c.node_hi
