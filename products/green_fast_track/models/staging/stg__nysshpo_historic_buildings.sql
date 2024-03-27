WITH points AS (
    SELECT * FROM {{ source('recipe_sources', 'nysshpo_historic_buildings_points') }}
),
polygons AS (
    SELECT * FROM {{ source('recipe_sources', 'nysshpo_historic_buildings_polygons') }}
)

-- Note: These two datasets have entirely distinct buildings, based on the usnnum
SELECT
    usnnum,
    usnname,
    wkb_geometry AS geom,
    eligibilitydesc
FROM points
UNION
SELECT
    usnnum,
    usnname,
    wkb_geometry AS geom,
    eligibilitydesc
FROM polygons
