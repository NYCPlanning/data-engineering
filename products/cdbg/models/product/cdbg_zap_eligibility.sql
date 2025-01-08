WITH eligibility AS (
    SELECT * FROM {{ ref("int__zap_projects_tracts") }}
),

zap_projects AS (
    SELECT
        ROW_NUMBER() OVER () AS project_row_number,
        *
    FROM {{ ref("stg__zap_projects") }}
)

SELECT
    zap.project_id,
    zap.bbls AS all_zap_bbls,
    ARRAY_TO_STRING(e.bbls, ', ') AS distinct_bbls,
    COALESCE(e.entirely_within_quarter_mile_of_eligible_tract, FALSE) AS entirely_within_quarter_mile_of_eligible_tract,
    COALESCE(e.entirely_within_half_mile_of_eligible_tract, FALSE) AS entirely_within_half_mile_of_eligible_tract,
    COALESCE(e.partially_within_quarter_mile_of_eligible_tract, FALSE) AS partially_within_quarter_mile_of_eligible_tract,
    COALESCE(e.partially_within_half_mile_of_eligible_tract, FALSE) AS partially_within_half_mile_of_eligible_tract
FROM zap_projects AS zap
LEFT JOIN eligibility AS e
    ON zap.project_id = e.project_id
ORDER BY project_row_number
