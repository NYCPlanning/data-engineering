-- int_buffers__nysshpo_historic_buildings

WITH pluto AS (
    SELECT
        geom,
        bbl
    FROM {{ ref('stg__pluto') }}
),

building_points AS (
    SELECT * FROM {{ ref('stg__nysshpo_historic_buildings') }}
),

points_with_maybe_pluto_geom AS (
    SELECT
        building_points.variable_type,
        building_points.variable_id,
        COALESCE(pluto.geom, building_points.raw_geom) AS raw_geom
    FROM building_points
    LEFT JOIN pluto ON ST_INTERSECTS(pluto.geom, building_points.raw_geom)
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 90) AS buffer
FROM points_with_maybe_pluto_geom
