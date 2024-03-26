-- int_buffers__nys_parks_properties.sql

WITH parks_properties AS (
    SELECT * FROM {{ ref("stg__nys_parks_properties") }}
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 75) AS buffer
FROM parks_properties
