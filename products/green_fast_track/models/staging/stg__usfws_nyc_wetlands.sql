{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "usfws_nyc_wetlands"), left_by="geometry") }}
)

SELECT 
    "usfws_wetlands" AS variable_type,
    row_number() AS variable_id,
    geom
FROM clipped_to_nyc
