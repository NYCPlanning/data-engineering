{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT 
    'freshwater_wetlands' AS variable_type,
    name || ' ' || wetid AS variable_id,
    wkb_geometry AS geom
FROM {{ source("recipe_sources", "nysdec_freshwater_wetlands") }}
WHERE name IN ('Bronx', 'Kings', 'Queens', 'Richmond')
