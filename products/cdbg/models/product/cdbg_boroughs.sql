WITH boros AS (
    SELECT * FROM {{ ref("int__boros") }}
),

nyc AS (
    SELECT
        'New York City' AS borough_name,
        NULL::int AS borough_code,
        SUM(total_floor_area) AS total_floor_area,
        SUM(residential_floor_area) AS residential_floor_area,
        SUM(residential_floor_area) / SUM(total_floor_area) * 100 AS residential_floor_area_percentage,
        SUM(potential_lowmod_population) AS potential_lowmod_population,
        SUM(low_mod_income_population) AS low_mod_income_population,
        SUM(low_mod_income_population) / SUM(potential_lowmod_population) * 100 AS low_mod_income_population_percentage
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
    (ROUND(total_floor_area::numeric))::bigint AS total_floor_area,
    (ROUND(residential_floor_area::numeric))::bigint AS residential_floor_area,
    ROUND(residential_floor_area_percentage::numeric, 2) AS residential_floor_area_percentage,
    low_mod_income_population::bigint,
    potential_lowmod_population::bigint,
    ROUND(low_mod_income_population_percentage::numeric, 2) AS low_mod_income_population_percentage
FROM boros_with_nyc
ORDER BY borough_code IS NOT NULL, borough_code
