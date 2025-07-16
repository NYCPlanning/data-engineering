SELECT
    row_number() OVER () AS "OBJECTID",
    borough_name,
    borough_code,
    bct2020,
    ct2020,
    ctlabel,
    round(total_floor_area::numeric)::integer AS total_floor_area,
    round(residential_floor_area::numeric)::integer AS residential_floor_area,
    round(residential_floor_area_percentage::numeric, 2) AS residential_floor_area_percentage,
    potential_lowmod_population::integer,
    low_mod_income_population::integer,
    round(low_mod_income_population_percentage::numeric, 2) AS low_mod_income_population_percentage,
    eligibility_flag,
    eligibility,
    geom
FROM {{ ref("int__tracts") }}
ORDER BY bct2020
