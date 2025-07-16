WITH eligibility AS (
    SELECT * FROM {{ ref("int__zap_projects_tracts") }}
),

zap_projects AS (
    SELECT
        row_number() OVER ()::integer AS project_row_number,
        *
    FROM {{ ref("stg__zap_projects") }}
)

SELECT
    zap.*,
    array_to_string(e.bbls, ', ') AS distinct_bbls,
    {{ bool_to_str("e.entirely_within_quarter_mile_of_eligible_tract") }} AS entirely_within_quarter_mile_of_eligible_tract, --noqa: LT05
    {{ bool_to_str("e.entirely_within_half_mile_of_eligible_tract") }} AS entirely_within_half_mile_of_eligible_tract,
    {{ bool_to_str("e.partially_within_quarter_mile_of_eligible_tract") }} AS partially_within_quarter_mile_of_eligible_tract, --noqa: LT05
    {{ bool_to_str("e.partially_within_half_mile_of_eligible_tract") }} AS partially_within_half_mile_of_eligible_tract
FROM zap_projects AS zap
LEFT JOIN eligibility AS e
    ON zap.project_id = e.project_id
ORDER BY project_row_number
