-- stg__dpr_schoolyard_to_playgrounds.sql

WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'dpr_schoolyard_to_playgrounds') }}
),

selected_columns AS (
    SELECT
        'schoolyard_to_playgrounds' AS variable_type,
        gispropnum,
        location,
        gispropnum || '-' || location AS variable_id,
        ST_Transform(wkb_geometry, 2263) AS raw_geom
    FROM source
)

SELECT *
FROM selected_columns
