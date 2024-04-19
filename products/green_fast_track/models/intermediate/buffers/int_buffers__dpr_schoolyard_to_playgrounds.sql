-- int_buffers__dpr_schoolyard_to_playgrounds.sql

WITH schoolyards AS (
    SELECT * FROM {{ ref("stg__dpr_schoolyard_to_playgrounds") }}
),

modified_id AS (
    SELECT
        variable_type,
        raw_geom,
        gispropnum || '-' || location AS variable_id
    FROM schoolyards
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 200) AS buffer
FROM modified_id
