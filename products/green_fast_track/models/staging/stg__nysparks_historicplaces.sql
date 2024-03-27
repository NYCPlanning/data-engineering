WITH historic_places AS (
    SELECT * FROM {{ source('recipe_sources', 'nysparks_historicplaces') }}
)

SELECT
    ST_TRANSFORM(wkb_geometry, 2263) AS geom,
    resource_name,
    county,
    national_register_number
FROM historic_places
