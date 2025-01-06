WITH block_groups AS (
    SELECT * FROM {{ ref("int__block_groups") }}
),

eligibility_calculation AS (
    SELECT
        geoid,
        borough_name,
        borough_code,
        tract,
        block_group,
        round(total_floor_area::numeric)::integer AS total_floor_area,
        round(residential_floor_area::numeric)::integer AS residential_floor_area,
        round(residential_floor_area_percentage::numeric, 2) AS residential_floor_area_percentage,
        low_mod_income_population,
        round(low_mod_income_population_percentage::numeric, 2) AS low_mod_income_population_percentage,
        low_mod_income_population_percentage >= 51 AND residential_floor_area_percentage >= 50 AS eligibility_flag
    FROM block_groups
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
