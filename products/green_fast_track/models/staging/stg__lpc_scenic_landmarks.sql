SELECT
    'scenic_landmarks' AS variable_type,
    lp_number || '-' || scen_lm_na AS variable_id,
    -- Socrata serves this GeoJSON with State Plane coordinates despite the format's
    -- WGS84 contract, so ingest labels it 4326. Relabel, don't reproject.
    ST_SETSRID(wkb_geometry, 2263) AS raw_geom,
    NULL AS buffer
FROM {{ source('recipe_sources', 'lpc_scenic_landmarks') }}
