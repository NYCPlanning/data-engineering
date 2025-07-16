WITH boros AS (
    SELECT * FROM {{ ref("int__boros") }}
),

nyc AS (
    SELECT
        'New York City' AS borough_name,
        NULL::int AS borough_code,
        sum(total_floor_area) AS total_floor_area,
        sum(residential_floor_area) AS residential_floor_area,
        sum(residential_floor_area) / sum(total_floor_area) * 100 AS residential_floor_area_percentage,
        sum(potential_lowmod_population) AS potential_lowmod_population,
        sum(low_mod_income_population) AS low_mod_income_population,
        sum(low_mod_income_population) / sum(potential_lowmod_population) * 100 AS low_mod_income_population_percentage
    FROM boros
),

boros_with_nyc AS (
    SELECT * FROM nyc
    UNION ALL
    SELECT * FROM boros
)

SELECT
    borough_name,
    borough_code,
    (round(total_floor_area::numeric))::bigint AS total_floor_area,
    (round(residential_floor_area::numeric))::bigint AS residential_floor_area,
    round(residential_floor_area_percentage::numeric, 2) AS residential_floor_area_percentage,
    low_mod_income_population::bigint,
    potential_lowmod_population::bigint,
    round(low_mod_income_population_percentage::numeric, 2) AS low_mod_income_population_percentage
FROM boros_with_nyc
ORDER BY borough_code IS NOT NULL, borough_code
