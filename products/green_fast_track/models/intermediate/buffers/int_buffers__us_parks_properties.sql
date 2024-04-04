-- int_buffers__us_parks_properties.sql

WITH usparks_properties AS (
    SELECT * FROM {{ ref("stg__us_parks_properties") }}
),

modified_id AS (
    SELECT
        variable_type,
        raw_geom,
        gnis_id || '-' || parkname AS variable_id
    FROM usparks_properties
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    st_buffer(raw_geom, 200) AS buffer
FROM modified_id
