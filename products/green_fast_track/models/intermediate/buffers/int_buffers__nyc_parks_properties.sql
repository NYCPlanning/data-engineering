-- int_buffers__nyc_parks_properties.sql

WITH parks_properties AS (
    SELECT * FROM {{ ref("stg__nyc_parks_properties") }}
),

modified_id AS (
    SELECT
        variable_type,
        raw_geom,
        gispropnum || '-' || name311 AS variable_id
    FROM parks_properties
),

-- some park properties are listed multiple times due to different geometries. Group them
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
