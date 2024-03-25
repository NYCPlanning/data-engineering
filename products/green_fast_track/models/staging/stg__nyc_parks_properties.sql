-- stg__nyc_parks_properties.sql
WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'dpr_parksproperties') }}
),

selected_columns AS (
    SELECT 'nyc_parks_properties' AS variable_type,
        objectid AS variable_id,
        typecategory,
        st_transform(wkb_geometry, 2263) AS raw_geom
    FROM source
),

-- filter records by a list of typecategory values identified in nyc_parks_properties_categories
filtered AS (
    SELECT s.variable_type,
        s.variable_id,
        s.raw_geom
    FROM selected_columns AS s
        JOIN {{ ref('nyc_parks_properties_categories') }} AS n ON s.typecategory = n.typecategory
    WHERE n.allowed = TRUE
)

SELECT *
FROM filtered
