WITH lpc_dist_areas AS (
    SELECT
        area_name,
        lp_number,
        ST_GEOMFROMTEXT(the_geom, '2263') AS raw_geom
    FROM {{ source('recipe_sources', 'lpc_historic_district_areas') }}
)

SELECT
    lp_number || '-' || area_name AS variable_id,
    'historic_district' AS variable_type,
    raw_geom AS raw_geom,
    raw_geom AS buffer
FROM lpc_dist_areas
