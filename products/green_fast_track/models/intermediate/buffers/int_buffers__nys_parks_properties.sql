-- int_buffers__nys_parks_properties.sql

WITH parks_properties AS (
    SELECT * FROM {{ ref("stg__nys_parks_properties") }}
),

modified_id AS (
    SELECT
        variable_type,
        raw_geom,
        uid || '-' || name AS variable_id
    FROM parks_properties
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 200) AS buffer
FROM modified_id
