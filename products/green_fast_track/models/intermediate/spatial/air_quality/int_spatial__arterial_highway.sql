WITH arterial_highways AS (
    SELECT * FROM {{ ref("stg__dcm_arterial_highways") }}
)

SELECT
    'arterial_highway' AS flag_id_field_name,
    'arterial_highways' AS variable_type,
    name AS variable_id,
    wkb_geometry AS raw_geom,
    ST_Buffer(wkb_geometry, 200) AS buffer_geom
FROM arterial_highways
