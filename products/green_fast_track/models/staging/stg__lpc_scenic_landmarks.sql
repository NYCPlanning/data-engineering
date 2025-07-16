SELECT
    'scenic_landmarks' AS variable_type,
    lp_number || '-' || scen_lm_na AS variable_id,
    st_transform(wkb_geometry, 2263) AS raw_geom,
    NULL AS buffer
FROM {{ source('recipe_sources', 'lpc_scenic_landmarks') }}
