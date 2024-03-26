-- int_buffers__waterfront_access_wpaa.sql

WITH wpaa AS (
    SELECT * FROM {{ ref("stg__waterfront_access_wpaa") }}
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 75) AS buffer
FROM wpaa
