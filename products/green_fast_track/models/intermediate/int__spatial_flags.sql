-- int__spatial_flags.sql

WITH buffered_flags AS (
    SELECT
        variable_type,
        variable_id,
        buffer,
        raw_geom
    FROM
        {{ ref('int__all_buffers') }}
),

pluto AS (
    SELECT
        bbl,
        geom AS bbl_geom
    FROM {{ ref('stg__pluto') }}
),

-- Spatially join pluto with buffered flags. 
-- Calculate distance to raw geometry of the joined flags
final AS (
    SELECT
        p.bbl,
        b.variable_type,
        b.variable_id,
        ST_DISTANCE(p.bbl_geom, b.raw_geom) AS distance
    FROM buffered_flags AS b INNER JOIN pluto AS p
        ON ST_INTERSECTS(b.buffer, p.bbl_geom)
)

SELECT * FROM final
