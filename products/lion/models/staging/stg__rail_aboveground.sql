{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['centroid'], 'type': 'gist'},
      {'columns': ['segmentid']}
    ]
) }}


SELECT
    segmentid,
    geom,
    ST_CENTROID(geom) AS centroid,
    'railroad' AS rail_type
FROM {{ source("recipe_sources", "dcp_cscl_subways") }}
UNION
SELECT
    segmentid,
    geom,
    ST_CENTROID(geom) AS centroid,
    'subway' AS rail_type
FROM {{ source("recipe_sources", "dcp_cscl_railroads") }}
