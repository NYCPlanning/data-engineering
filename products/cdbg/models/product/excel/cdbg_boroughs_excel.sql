SELECT
    borough_name AS "Borough Name",
    total_floor_area AS "Total Floor Area",
    residential_floor_area AS "Total Residential Floor Area",
    residential_floor_area_percentage AS "Percent Floor Area Residential",
    potential_lowmod_population AS "Total Population*",
    low_mod_income_population AS "Number of Low and Moderate Income Persons",
    low_mod_income_population_percentage AS "Percent Low and Moderate Income Persons"
FROM {{ ref("cdbg_boroughs") }}
