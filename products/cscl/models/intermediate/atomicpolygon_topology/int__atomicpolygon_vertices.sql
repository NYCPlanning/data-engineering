{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['atomicid']},
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

-- Every ring vertex of every atomic polygon, exploded out of stg__atomicpolygons.geom.
-- ST_DumpPoints on a multipolygon returns path = [part_index, ring_index, point_ordinal];
-- ring_index 1 is the exterior ring, 2+ are interior rings (holes), and point_ordinal is
-- the point's position within that ring (first == last, since rings are closed).
-- POC for a points-based topology: see products/cscl chat log 2026-09-20.
WITH dumped AS (
    SELECT
        atomicid,
        (ST_DumpPoints(geom)).path AS path,
        (ST_DumpPoints(geom)).geom AS geom
    FROM {{ ref('stg__atomicpolygons') }}
)

SELECT
    atomicid,
    path,
    path[1] AS part_index,
    path[2] AS ring_index,
    path[3] AS point_ordinal,
    geom
FROM dumped
