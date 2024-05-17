WITH historic_places AS (
    SELECT * FROM {{ source('recipe_sources', 'nysparks_historicplaces_esri') }}
)

SELECT
    ST_TRANSFORM(wkb_geometry, 2263) AS geom,
    historicname,
    citytown,
    countyname,
    nrnum
FROM historic_places
