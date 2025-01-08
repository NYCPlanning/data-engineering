WITH lots_eligibility AS (
    SELECT * FROM {{ ref("int__zap_lots_eligiblity") }}
),

projects_eligibility AS (
    SELECT * FROM {{ ref("int__zap_project_eligiblity") }}
),

zap_projects AS (
    SELECT * FROM {{ ref("stg__zap_projects") }}
),

project_lot_arrays AS (
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

project_lot_eligiblity_flags AS (
    SELECT
        project_id,
        bbls,
        true = ANY(within_quarter_mile_array) AS a_lot_quarter_mile,
        true = ANY(within_half_mile_array) AS a_lot_half_mile
    FROM project_lot_arrays
),

eligiblity_flags AS (
    SELECT
        project_lot_eligiblity_flags.project_id,
        project_lot_eligiblity_flags.bbls,
        projects_eligibility.contained_quarter_mile AS all_lots_quarter_mile,
        projects_eligibility.contained_half_mile AS all_lots_half_mile,
        project_lot_eligiblity_flags.a_lot_quarter_mile,
        project_lot_eligiblity_flags.a_lot_half_mile
    FROM project_lot_eligiblity_flags
    LEFT JOIN projects_eligibility
        ON project_lot_eligiblity_flags.project_id = projects_eligibility.project_id
),

eligiblity AS (
    SELECT
        project_id,
        bbls,
        CASE
            WHEN all_lots_quarter_mile THEN 'Yes'
            ELSE 'No'
        END AS entirely_within_quarter_mile_of_eligible_tract,
        CASE
            WHEN all_lots_half_mile THEN 'Yes'
            ELSE 'No'
        END AS entirely_within_half_mile_of_eligible_tract,
        CASE
            WHEN a_lot_quarter_mile THEN 'Yes'
            ELSE 'No'
        END AS partialy_within_quarter_mile_of_eligible_tract,
        CASE
            WHEN a_lot_half_mile THEN 'Yes'
            ELSE 'No'
        END AS partialy_within_half_mile_of_eligible_tract
    FROM eligiblity_flags
),

original_order AS (
    SELECT
        zap_projects.project_id,
        zap_projects.bbls AS all_zap_bbls,
        eligiblity.bbls AS distinct_bbls,
        eligiblity.entirely_within_quarter_mile_of_eligible_tract,
        eligiblity.entirely_within_half_mile_of_eligible_tract,
        eligiblity.partialy_within_quarter_mile_of_eligible_tract,
        eligiblity.partialy_within_half_mile_of_eligible_tract
    FROM zap_projects
    LEFT JOIN eligiblity
        ON zap_projects.project_id = eligiblity.project_id
)

SELECT * FROM original_order
