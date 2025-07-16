WITH lpc_diST_Areas AS (
    SELECT
        area_name,
        lp_number,
        ST_Transform(wkb_geometry, 2263) AS raw_geom
    FROM {{ source('recipe_sources', 'lpc_historic_district_areas') }}
)

SELECT
    'nyc_historic_districts' AS variable_type,
    lp_number || '-' || area_name AS variable_id,
    raw_geom,
    NULL AS buffer
FROM lpc_diST_Areas
