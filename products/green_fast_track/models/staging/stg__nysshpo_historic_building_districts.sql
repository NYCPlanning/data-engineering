-- stg__nysshpo_historic_building_districts

{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['raw_geom'], 'type': 'gist'},
    ]
) }}

WITH polys_clipped AS (
    {{ clip_to_geom(source('recipe_sources', 'nysshpo_historic_buildings_polygons'), left_by='geom' ) }}
    WHERE eligibilitydesc IN ('Eligible', 'Listed')
),

final AS (
    SELECT
        'nys_historic_districts' AS variable_type,
        usnnum || COALESCE('-' || usnname, '') AS variable_id,
        ST_TRANSFORM(geom, 2263) AS raw_geom,
        NULL AS buffer
    FROM polys_clipped
)

SELECT * FROM final
