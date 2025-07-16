{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
      {'columns': ['nodeid']},
    ]
) }}

WITH segments AS (
    SELECT * FROM {{ ref("stg__segment_geoms") }}
),

nodes AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_nodes") }}
),

segment_endpoints AS (
    SELECT
        segmentid,
        'from' AS direction,
        ST_StartPoint(geom) AS geom
    FROM segments
    UNION ALL
    SELECT
        segmentid,
        'to' AS direction,
        ST_EndPoint(geom) AS geom
    FROM segments
)

SELECT
    seg.segmentid,
    seg.direction,
    nodes.nodeid
FROM segment_endpoints AS seg
INNER JOIN nodes ON ST_DWithin(seg.geom, nodes.geom, 0.01)
