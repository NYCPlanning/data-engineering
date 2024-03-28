{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "nysdec_priority_streams"), left_by="wkb_geometry") }}
)

SELECT 
    'priority_streams' AS variable_type,
    name AS variable_id,
    geom
FROM clipped_to_nyc
