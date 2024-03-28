{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "nysdec_freshwater_wetlands_checkzones"), left_by="wkb_geometry") }}
)

SELECT SELECT 
    'freshwater_wetland_checkzones' AS variable_type,
    objectid AS variable_id,
    wkb_geometry AS geom
FROM clipped_to_nyc
