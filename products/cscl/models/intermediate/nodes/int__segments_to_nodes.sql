{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['globalid']},
      {'columns': ['nodeid']},
    ]
) }}

WITH segments AS (
    SELECT * FROM {{ ref("int__segments") }}
),

nodes AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_nodes") }}
),

segment_endpoints AS (
    SELECT
        globalid,
        lionkey,
        segmentid,
        'from' AS direction,
        start_point AS geom
    FROM segments
    UNION ALL
    SELECT
        globalid,
        lionkey,
        segmentid,
        'to' AS direction,
        end_point AS geom
    FROM segments
)

SELECT
    seg.globalid,
    seg.lionkey,
    seg.segmentid,
    seg.direction,
    nodes.nodeid
FROM segment_endpoints AS seg
INNER JOIN nodes ON st_dwithin(seg.geom, nodes.geom, 0.0025)
