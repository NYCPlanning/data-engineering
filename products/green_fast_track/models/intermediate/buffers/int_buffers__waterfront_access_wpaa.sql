-- int_buffers__waterfront_access_wpaa.sql

WITH wpaa AS (
    SELECT * FROM {{ ref("stg__waterfront_access_wpaa") }}
),

filtered_status AS (
    SELECT
        variable_type,
        wpaa_id,
        name,
        raw_geom
    FROM wpaa
    WHERE UPPER(status) != 'CLOSED'
),

modified_id AS (
    SELECT
        variable_type,
        raw_geom,
        wpaa_id || '-' || name AS variable_id
    FROM filtered_status
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 200) AS buffer
FROM modified_id
