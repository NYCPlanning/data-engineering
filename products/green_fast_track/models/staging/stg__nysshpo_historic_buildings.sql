-- stg__nysshpo_historic_buildings

{{ config(
    materialized = 'table'
) }}

WITH points_clipped AS (
    {{ clip_to_geom(source('recipe_sources', 'nysshpo_historic_buildings_points'), left_by='geom' ) }}
    WHERE eligibilitydesc IN ('Eligible', 'Listed')
),

final AS (
    SELECT
        'nys_historic_buildings' AS variable_type,
        -- usnname is NULL in the postgres archive but '' (empty string) in duckdb's for the
        -- same underlying records -- NULLIF normalizes both to "no suffix" instead of leaving
        -- a trailing '-' on ~18k duckdb rows that postgres doesn't have
        usnnum || COALESCE('-' || NULLIF(usnname, ''), '') AS variable_id,
        {{ dcp_st_transform('geom', 2263) }} AS raw_geom
    FROM points_clipped
)

SELECT * FROM final
