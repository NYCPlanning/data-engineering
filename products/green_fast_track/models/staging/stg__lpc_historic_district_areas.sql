WITH lpc_dist_areas AS (
    SELECT
        area_name,
        lp_number,
        st_transform(wkb_geometry, 2263) AS raw_geom
    FROM {{ source('recipe_sources', 'lpc_historic_district_areas') }}
)

SELECT
    'nyc_historic_districts' AS variable_type,
    lp_number || '-' || area_name AS variable_id,
    raw_geom,
    NULL AS buffer
FROM lpc_dist_areas
