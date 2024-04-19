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
        st_transform(wkb_geometry, 2263) AS raw_geom
    FROM source
)

SELECT *
FROM selected_columns
