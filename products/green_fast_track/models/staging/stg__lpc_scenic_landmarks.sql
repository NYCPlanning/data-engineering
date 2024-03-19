SELECT
    'scenic_landmark' AS variable_type,
    lp_number || '-' || scen_lm_na AS variable_id,
    ST_TRANSFORM(wkb_geometry, 2263) AS raw_geom,
    ST_TRANSFORM(wkb_geometry, 2263) AS buffer
FROM {{ source('recipe_sources', 'lpc_scenic_landmarks') }}
