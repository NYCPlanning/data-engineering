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
    globalid,
    patrol_borough,
    shape_length,
    shape_area,
    geom AS raw_geom,
    LINEARIZE(geom) AS geom
FROM {{ source("recipe_sources", "dcp_cscl_nypdpatrolborough") }}
