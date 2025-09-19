-- stg__nysshpo_historic_buildings

{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['raw_geom'], 'type': 'gist'},
    ]
) }}

WITH points_clipped AS (
    {{ clip_to_geom(source('recipe_sources', 'nysshpo_historic_buildings_points'), left_by='geom' ) }}
    WHERE eligibilitydesc IN ('Eligible', 'Listed')
),

final AS (
    SELECT
        'nys_historic_buildings' AS variable_type,
        usnnum || COALESCE('-' || usnname, '') AS variable_id,
        ST_TRANSFORM(geom, 2263) AS raw_geom
    FROM points_clipped
)

SELECT * FROM final
