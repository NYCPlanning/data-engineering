{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['midpoint'], 'type': 'gist'},
      {'columns': ['segmentid']}
    ]
) }}


SELECT
    segmentid,
    geom,
    ST_LineInterpolatePoint(ST_LineMerge(geom), 0.5) AS midpoint,
    'subway' AS rail_type
FROM {{ source("recipe_sources", "dcp_cscl_subway") }}
WHERE row_type = '1'
UNION
SELECT
    segmentid,
    geom,
    ST_LineInterpolatePoint(ST_LineMerge(geom), 0.5) AS midpoint,
    'railroad' AS rail_type
FROM {{ source("recipe_sources", "dcp_cscl_rail") }}
WHERE row_type = '1' -- FYI: row_type == Right of Way type, and a value of 1 = "subterranean"
