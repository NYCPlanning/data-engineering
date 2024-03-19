WITH pluto AS (
    SELECT
        geom,
        bbl
    FROM {{ ref('stg__pluto') }}
),

----- POINTS
points_clipped AS (
    {{ clip_to_geom(source('recipe_sources', 'nysshpo_historic_buildings_points'), left_by='wkb_geometry' ) }}
    WHERE eligibilitydesc IN ('Eligible', 'Listed')
),
points_with_maybe_pluto_geom AS (
    SELECT
        points.usnnum AS usnnum,
        points.usnname AS usnname,
        COALESCE(pluto.geom, points.geom) AS geom
    FROM points_clipped AS points
    LEFT JOIN pluto ON ST_INTERSECTS(pluto.geom, points.geom)
    WHERE points.geom IS NOT NULL
),

----- POLYS
polys_clipped AS (
    {{ clip_to_geom(source('recipe_sources', 'nysshpo_historic_buildings_polygons'), left_by='wkb_geometry' ) }}
    WHERE eligibilitydesc IN ('Eligible', 'Listed')
)

SELECT
    usnnum || '-' || usnname AS variable_id,
    'historic_building' AS variable_type,
    geom AS raw_geom,
    ST_BUFFER(geom, 90) AS buffer
FROM points_with_maybe_pluto_geom
UNION
SELECT
    usnnum || '-' || usnname AS variable_id,
    'historic_building_district' AS variable_type,
    geom AS raw_geom,
    geom AS buffer
FROM polys_clipped
