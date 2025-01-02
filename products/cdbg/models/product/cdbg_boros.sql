WITH boros AS (
    SELECT * FROM {{ ref("int__boros") }}
),

eligibility_calculation AS (
    SELECT
        *,
        low_mod_income_population_percentage > 51 AND residential_floor_area_percentage > 50 AS eligibility_flag
    FROM boros
),

eligibility AS (
    SELECT
        *,
        CASE
            WHEN eligibility_flag THEN 'CD Eligible'
            ELSE 'Ineligible'
        END AS eligibility
    FROM eligibility_calculation
)

SELECT * FROM eligibility
