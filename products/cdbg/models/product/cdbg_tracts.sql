SELECT
    ROW_NUMBER() OVER () AS "OBJECTID",
    borough_name,
    borough_code,
    bct2020,
    ct2020,
    ctlabel,
    ROUND(total_floor_area::numeric)::integer AS total_floor_area,
    ROUND(residential_floor_area::numeric)::integer AS residential_floor_area,
    ROUND(residential_floor_area_percentage::numeric, 2) AS residential_floor_area_percentage,
    potential_lowmod_population::integer,
    low_mod_income_population::integer,
    ROUND(low_mod_income_population_percentage::numeric, 2) AS low_mod_income_population_percentage,
    eligibility_flag,
    eligibility,
    geom
FROM {{ ref("int__tracts") }}
ORDER BY bct2020
