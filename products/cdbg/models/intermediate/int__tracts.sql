WITH block_groups AS (
    SELECT * FROM {{ ref("int__block_groups") }}
),

raw_tracts AS (
    SELECT * FROM {{ ref("stg__census_tracts") }}
),

grouped_tracts AS (
    SELECT
        left(geoid, -1) AS geoid,
        sum(total_floor_area) AS total_floor_area,
        sum(residential_floor_area) AS residential_floor_area,
        sum(potential_lowmod_population) AS potential_lowmod_population,
        sum(low_mod_income_population) AS low_mod_income_population
    FROM block_groups
    GROUP BY left(geoid, -1)
),

tracts AS (
    SELECT
        ct.geoid,
        ct.borough_name,
        ct.borough_code,
        ct.bct2020,
        ct.ct2020,
        ct.ctlabel,
        bg2t.total_floor_area,
        bg2t.residential_floor_area,
        bg2t.potential_lowmod_population,
        bg2t.low_mod_income_population,
        ct.geom
    FROM raw_tracts AS ct
    INNER JOIN grouped_tracts AS bg2t ON ct.geoid = bg2t.geoid
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
