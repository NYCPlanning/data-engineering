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
        sum(total_population) AS total_population,
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
            WHEN total_population = 0
                THEN 0
            ELSE (low_mod_income_population / total_population) * 100
        END AS low_mod_income_population_percentage
    FROM tracts
)

SELECT * FROM tracts_calculation
