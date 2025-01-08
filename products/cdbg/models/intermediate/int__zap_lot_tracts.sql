WITH eligible_tracts AS (
    SELECT * FROM {{ ref('int__eligible_tracts') }}
),

lots AS (
    SELECT * FROM {{ ref('int__zap_lots') }}
)

SELECT
    lots.project_id,
    lots.bbl,
    lots.geom AS lot_geom,
    eligible_tracts.geoid,
    eligible_tracts.half_mile_buffer_geom AS tract_buffer_half_mile,
    eligible_tracts.quarter_mile_buffer_geom AS tract_buffer_quarter_mile,
    ST_INTERSECTS(lots.geom, eligible_tracts.half_mile_buffer_geom) AS intersects_half_mile,
    ST_INTERSECTS(lots.geom, eligible_tracts.quarter_mile_buffer_geom) AS intersects_quarter_mile
FROM lots
LEFT JOIN eligible_tracts
    ON ST_INTERSECTS(lots.geom, eligible_tracts.half_mile_buffer_geom)
