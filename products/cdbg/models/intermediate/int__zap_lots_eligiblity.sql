WITH lot_tracts AS (
    SELECT * FROM {{ ref("int__zap_lot_tracts") }}
),

lots AS (
    SELECT * FROM {{ ref('int__zap_lots') }}
),

lots_with_a_quarter_mile_intersection AS (
    SELECT DISTINCT
        project_id,
        bbl,
        intersection_type
    FROM lot_tracts
    WHERE intersection_type = 'quarter_mile'
),

lots_with_a_half_mile_intersection AS (
    SELECT DISTINCT
        project_id,
        bbl,
        intersection_type
    FROM lot_tracts
    WHERE intersection_type = 'half_mile'
),

quarter_mile AS (
    SELECT
        lots.project_id,
        lots.bbl,
        coalesce(lots_with_a_quarter_mile_intersection.bbl IS NOT NULL, TRUE) AS within_quarter_mile
    FROM lots
    LEFT JOIN lots_with_a_quarter_mile_intersection
        ON
            lots.project_id = lots_with_a_quarter_mile_intersection.project_id
            AND lots.bbl = lots_with_a_quarter_mile_intersection.bbl
),

quarter_and_half_mile AS (
    SELECT
        quarter_mile.*,
        coalesce(lots_with_a_half_mile_intersection.bbl IS NOT NULL, TRUE) AS within_half_mile
    FROM quarter_mile
    LEFT JOIN lots_with_a_half_mile_intersection
        ON
            quarter_mile.project_id = lots_with_a_half_mile_intersection.project_id
            AND quarter_mile.bbl = lots_with_a_half_mile_intersection.bbl
)

SELECT * FROM quarter_and_half_mile
ORDER BY
    project_id ASC,
    bbl ASC
