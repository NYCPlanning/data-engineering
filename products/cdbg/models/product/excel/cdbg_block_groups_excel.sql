WITH block_groups AS (
    SELECT * FROM {{ ref("cdbg_block_groups") }}
),

SELECT
    borough_name AS "Borough Name",
    borough_code AS "Borough Code",
    bct2020 AS "BoroCT",
    ct2020 AS "Census Tract Long",
    block_group AS "Block Group",
    bctbg2020 AS "BoroCTBG",
    total_floor_area AS "Total Floor Area",
    residential_floor_area AS "Total Residential Floor Area",
    residential_floor_area_percentage AS "% of Floor Area Residential",
    potential_lowmod_population AS "Total Population",
    low_mod_income_population AS "Number of Low-/ Moderate-Income Persons",
    low_mod_income_population_percentage AS "% of Low-/ Moderate-Income Persons",
    eligibility AS "Eligibility"
FROM block_groups
