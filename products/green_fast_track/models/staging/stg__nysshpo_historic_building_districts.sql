-- stg__nysshpo_historic_building_districts

{{ config(
    materialized = 'table'
) }}

WITH polys_clipped AS (
    {{ clip_to_geom(source('recipe_sources', 'nysshpo_historic_buildings_polygons'), left_by='geom' ) }}
    WHERE eligibilitydesc IN ('Eligible', 'Listed')
),

final AS (
    SELECT
        'nys_historic_districts' AS variable_type,
        -- usnname is NULL in the postgres archive but '' (empty string) in duckdb's for the
        -- same underlying records -- NULLIF normalizes both to "no suffix" instead of leaving
        -- a trailing '-' on rows duckdb has that postgres doesn't
        usnnum || COALESCE('-' || NULLIF(usnname, ''), '') AS variable_id,
        {{ dcp_st_transform('geom', 2263) }} AS raw_geom,
        NULL AS buffer
    FROM polys_clipped
)

SELECT * FROM final
