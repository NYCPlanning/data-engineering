WITH lpc_scenic_landmarks AS (
    SELECT * FROM {{ source('recipe_sources', 'lpc_scenic_landmarks') }}
)

SELECT
    'scenic_landmark' AS variable_type,
    scen_lm_na AS variable_id,
    ST_TRANSFORM(wkb_geometry, 2263) AS raw_geom,
    ST_BUFFER(ST_TRANSFORM(wkb_geometry, 2263), 90) AS buffer
FROM lpc_scenic_landmarks
