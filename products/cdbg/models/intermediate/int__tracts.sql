WITH block_groups AS (
    SELECT
        *,
        left(geoid, -1) AS tract_id
    FROM {{ ref("int__block_groups") }}
),

tracts AS (
    SELECT
        tract_id AS geoid,
        borough_name,
        borough_code,
        sum(total_floor_area) AS total_floor_area,
        sum(residential_floor_area) AS residential_floor_area,
        sum(potential_lowmod_population) AS potential_lowmod_population,
        sum(low_mod_income_population) AS low_mod_income_population
    FROM block_groups
    GROUP BY
        tract_id,
        borough_name,
        borough_code
),

tracts_calculation AS (
    SELECT
        *,
        CASE
            WHEN total_floor_area = 0
                THEN 0
            ELSE (residential_floor_area / total_floor_area) * 100
        END AS residential_floor_area_percentage,
        CASE
            WHEN potential_lowmod_population = 0
                THEN 0
            ELSE (low_mod_income_population / potential_lowmod_population) * 100
        END AS low_mod_income_population_percentage
    FROM tracts
),

eligibility_calculation AS (
    SELECT
        *,
        low_mod_income_population_percentage >= 51 AND residential_floor_area_percentage >= 50 AS eligibility_flag
    FROM tracts_calculation
),

eligibility_labels AS (
    SELECT
        *,
        CASE
            WHEN eligibility_flag THEN 'CD Eligible'
            ELSE 'Ineligible'
        END AS eligibility
    FROM eligibility_calculation
)

SELECT * FROM eligibility_labels
