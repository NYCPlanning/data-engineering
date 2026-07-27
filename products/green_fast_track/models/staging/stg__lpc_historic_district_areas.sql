WITH lpc_dist_areas AS (
    SELECT
        area_name,
        lp_number,
        -- Socrata serves this GeoJSON with State Plane coordinates despite the format's
        -- WGS84 contract, so ingest labels it 4326. Relabel, don't reproject.
        ST_SETSRID(wkb_geometry, 2263) AS raw_geom
    FROM {{ source('recipe_sources', 'lpc_historic_district_areas') }}
)

SELECT
    'nyc_historic_districts' AS variable_type,
    lp_number || '-' || area_name AS variable_id,
    raw_geom,
    NULL AS buffer
FROM lpc_dist_areas
