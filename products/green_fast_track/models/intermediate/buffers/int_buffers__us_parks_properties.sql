-- int_buffers__us_parks_properties.sql

WITH usparks_properties AS (
    SELECT * FROM {{ ref("stg__nys_parks_properties") }}
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 75) AS buffer
FROM usparks_properties
