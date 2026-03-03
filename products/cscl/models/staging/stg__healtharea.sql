{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    created_by,
    created_date,
    modified_by,
    modified_date,
    healtharea,
    health_ct_district,
    borough,
    globalid,
    shape_length,
    shape_area,
    geom AS raw_geom,
    LINEARIZE(geom) AS geom
FROM {{ source("recipe_sources", "dcp_cscl_healtharea") }}
