-- int_buffers__waterfront_access_pow.sql

WITH pow AS (
    SELECT * FROM {{ ref("stg__waterfront_access_pow") }}
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 200) AS buffer
FROM pow
