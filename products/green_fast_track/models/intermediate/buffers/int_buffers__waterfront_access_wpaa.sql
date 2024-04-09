-- int_buffers__waterfront_access_wpaa.sql

WITH wpaa AS (
    SELECT * FROM {{ ref("stg__waterfront_access_wpaa") }}
),

modified_id AS (
    SELECT
        variable_type,
        raw_geom,
        wpaa_id || '-' || wpaa_name AS variable_id
    FROM wpaa
),

-- some areas are listed multiple times due to different geometries. Group them
grouped_by_variable_id AS (
    SELECT
        variable_type,
        variable_id,
        ST_UNION(raw_geom) AS raw_geom
    FROM modified_id
    GROUP BY variable_type, variable_id
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 200) AS buffer
FROM grouped_by_variable_id
