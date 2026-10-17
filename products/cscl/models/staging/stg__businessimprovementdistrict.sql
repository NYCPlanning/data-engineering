{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    globalid,
    bidid,
    bid,
    borough,
    st_makevalid(geom) AS geom,
    geom AS raw_geom
FROM {{ source("recipe_sources", "dcp_cscl_businessimprovementdistrict") }}
