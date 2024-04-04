-- int_buffers__nyc_parks_properties.sql

WITH parks_properties AS (
    SELECT * FROM {{ ref("stg__nyc_parks_properties") }}
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 200) AS buffer
FROM parks_properties
