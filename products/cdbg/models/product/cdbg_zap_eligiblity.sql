WITH lots_eligibility AS (
    SELECT * FROM {{ ref('int__zap_lots_eligiblity') }}
),

zap_projects AS (
    SELECT * FROM {{ ref("stg__zap_projects") }}
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
),

original_order AS (
    SELECT
        zap_projects.project_id,
        project_eligiblity.bbls AS all_zap_bbls,
        project_eligiblity.bbls AS distinct_bbls,
        project_eligiblity.all_lots_within_quarter_mile_of_eligible_tract,
        project_eligiblity.all_lots_within_half_mile_of_eligible_tract,
        project_eligiblity.a_lot_within_quarter_mile_of_eligible_tract,
        project_eligiblity.a_lot_within_half_mile_of_eligible_tract
    FROM zap_projects
    LEFT JOIN project_eligiblity
        ON zap_projects.project_id = project_eligiblity.project_id
)

SELECT * FROM original_order
