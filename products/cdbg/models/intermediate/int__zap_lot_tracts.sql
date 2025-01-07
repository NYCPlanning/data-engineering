WITH eligible_tracts AS (
    SELECT * FROM {{ ref('int__eligible_tracts') }}
),

lots AS (
    SELECT * FROM {{ ref('int__zap_lots') }}
),

intersections_quarter_mile AS (
    SELECT
        lots.project_id,
        lots.bbl,
        lots.geom AS lot_geom,
        eligible_tracts.geoid,
        eligible_tracts.quarter_mile_buffer_geom AS tract_buffer_geom,
        'quarter_mile' AS intersection_type
    FROM lots
    INNER JOIN eligible_tracts
        ON ST_INTERSECTS(lots.geom, eligible_tracts.quarter_mile_buffer_geom)
),

intersections_half_mile AS (
    SELECT
        lots.project_id,
        lots.bbl,
        lots.geom AS lot_geom,
        eligible_tracts.geoid,
        eligible_tracts.half_mile_buffer_geom AS tract_buffer_geom,
        'half_mile' AS intersection_type
    FROM lots
    INNER JOIN eligible_tracts
        ON ST_INTERSECTS(lots.geom, eligible_tracts.half_mile_buffer_geom)
),

intersections AS (
    SELECT * FROM intersections_quarter_mile
    UNION ALL
    SELECT * FROM intersections_half_mile
)

SELECT * FROM intersections
ORDER BY
    project_id ASC,
    bbl ASC,
    geoid ASC
