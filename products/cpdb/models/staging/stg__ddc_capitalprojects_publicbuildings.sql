SELECT
    *,
    ST_TRANSFORM(wkb_geometry, 2263) AS geom
FROM {{ source('recipe_sources', 'ddc_capitalprojects_publicbuildings') }}
