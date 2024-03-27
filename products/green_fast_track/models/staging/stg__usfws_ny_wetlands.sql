{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "usfws_ny_wetlands"), left_by="shape") }}
)

SELECT * FROM clipped_to_nyc
