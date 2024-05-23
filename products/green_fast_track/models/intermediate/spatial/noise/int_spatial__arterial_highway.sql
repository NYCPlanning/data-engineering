WITH arterial_highways AS (
    SELECT * FROM {{ ref("stg__dcm_arterial_highways") }}
)

SELECT
    'arterial_highway' AS flag_variable_type,
    'arterial_highways' AS variable_type,
    name AS variable_id,
    wkb_geometry AS raw_geom,
    ST_BUFFER(wkb_geometry, 75) AS buffer_geom
FROM arterial_highways
