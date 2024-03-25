WITH historic_places AS (
    SELECT * FROM {{ source('recipe_sources', 'nysshpo_register_historic_places') }}
)

SELECT
    wkb_geometry AS geom,
    resource_name,
    county,
    sphinx_number AS national_register_num
FROM historic_places
