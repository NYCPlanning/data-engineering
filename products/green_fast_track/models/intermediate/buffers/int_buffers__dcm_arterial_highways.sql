WITH arterial_highways AS (
    SELECT * FROM {{ ref("stg__dcm_arterial_highways") }}
)

SELECT
    name AS variable_id,
    'arterial_highways' AS variable_type,
    wkb_geometry AS raw_geom,
    ST_BUFFER(wkb_geometry, 75) AS buffer
FROM arterial_highways
