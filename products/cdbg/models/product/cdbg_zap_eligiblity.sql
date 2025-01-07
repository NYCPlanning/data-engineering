WITH lots_eligibility AS (
    SELECT * FROM {{ ref('int__zap_lots_eligiblity') }}
),

project_arrays AS (
    SELECT
        project_id,
        ARRAY_TO_STRING(ARRAY_AGG(
            bbl
            ORDER BY bbl ASC
        ), ', ') AS bbls,
        ARRAY_AGG(DISTINCT within_quarter_mile) AS within_quarter_mile_array,
        ARRAY_AGG(DISTINCT within_half_mile) AS within_half_mile_array
    FROM lots_eligibility
    GROUP BY project_id
),

project_eligiblity_flags AS (
    SELECT
        project_id,
        bbls,
        true = ALL(within_quarter_mile_array) AS all_lots_quarter_mile,
        true = ALL(within_half_mile_array) AS all_lots_half_mile,
        true = ANY(within_quarter_mile_array) AS a_lot_quarter_mile,
        true = ANY(within_half_mile_array) AS a_lot_half_mile
    FROM project_arrays
),

project_eligiblity AS (
    SELECT
        project_id,
        bbls,
        CASE
            WHEN all_lots_quarter_mile THEN 'Yes'
            ELSE 'No'
        END AS all_lots_within_quarter_mile_of_eligible_tract,
        CASE
            WHEN all_lots_half_mile THEN 'Yes'
            ELSE 'No'
        END AS all_lots_within_half_mile_of_eligible_tract,
        CASE
            WHEN a_lot_quarter_mile THEN 'Yes'
            ELSE 'No'
        END AS a_lot_within_quarter_mile_of_eligible_tract,
        CASE
            WHEN a_lot_half_mile THEN 'Yes'
            ELSE 'No'
        END AS a_lot_within_half_mile_of_eligible_tract
    FROM project_eligiblity_flags
)

SELECT * FROM project_eligiblity
