WITH tracts AS (
    SELECT * FROM {{ ref("int__tracts") }}
),

boros AS (
    SELECT
        borough_name,
        borough_code,
        sum(total_floor_area) AS total_floor_area,
        sum(residential_floor_area) AS residential_floor_area,
        sum(total_population) AS total_population,
        sum(potential_lowmod_population) AS potential_lowmod_population,
        sum(low_mod_income_population) AS low_mod_income_population
    FROM tracts
    GROUP BY
        borough_name,
        borough_code
),

boro_calculation AS (
    SELECT
        *,
        (residential_floor_area / total_floor_area) * 100 AS residential_floor_area_percentage,
        (low_mod_income_population / potential_lowmod_population) * 100 AS low_mod_income_population_percentage
    FROM boros
)

SELECT * FROM boro_calculation
