WITH block_groups AS (
    SELECT * FROM {{ ref("int__block_groups") }}
),

renamed AS (
    SELECT
        borough_name AS "Borough Name",
        borough_code AS "Borough Code",
        bct2020 AS "BoroCT",
        ct2020 AS "Census Tract Long",
        right(geoid, 1) AS "Block Group",
        bctbg2020 AS "BoroCTBG",
        round(total_floor_area::numeric)::integer AS "Total Floor Area",
        round(residential_floor_area::numeric)::integer AS "Total Residential Floor Area",
        round(residential_floor_area_percentage::numeric, 2) AS "% of Floor Area Residential",
        potential_lowmod_population::integer AS "Total Population",
        low_mod_income_population::integer AS "Number of Low-/ Moderate-Income Persons",
        round(low_mod_income_population_percentage::numeric, 2) AS "% of Low-/ Moderate-Income Persons"
    FROM block_groups
)

SELECT
    *,
    CASE
        WHEN "% of Low-/ Moderate-Income Persons" >= 51 AND "% of Floor Area Residential" >= 50 THEN 'CD Eligible'
        ELSE 'Ineligible'
    END AS "Eligibility"
FROM renamed
