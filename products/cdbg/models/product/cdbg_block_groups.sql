WITH block_groups AS (
    SELECT * FROM {{ ref("int__block_groups") }}
),

renamed AS (
    SELECT
        borough_name,
        borough_code,
        bct2020,
        ct2020,
        right(geoid, 1) AS block_group,
        bctbg2020,
        round(total_floor_area::numeric)::integer AS total_floor_area,
        round(residential_floor_area::numeric)::integer AS residential_floor_area,
        round(residential_floor_area_percentage::numeric, 2) AS residential_floor_area_percentage,
        potential_lowmod_population::integer,
        low_mod_income_population::integer,
        round(low_mod_income_population_percentage::numeric, 2) AS low_mod_income_population_percentage,
        low_mod_income_population_percentage >= 51 AND residential_floor_area_percentage >= 50 AS eligibility_flag
    FROM block_groups
)

SELECT
    *,
    CASE
        WHEN eligibility_flag THEN 'CD Eligible'
        ELSE 'Ineligible'
    END AS eligibility
FROM renamed
ORDER BY bctbg2020
