SELECT
    "OBJECTID",
    borough_code AS "BoroCode",
    bct2020 AS "BoroCT",
    total_floor_area AS "BldgArea",
    residential_floor_area AS "ResArea",
    residential_floor_area_percentage AS "Res_pct",
    potential_lowmod_population AS "TotalPop",
    low_mod_income_population AS "LowMod_Population",
    low_mod_income_population_percentage AS "Percent Low and Moderate Income Persons",
    eligibility AS "Eligibility",
    bct2020 AS "CT_text"
FROM {{ ref("cdbg_tracts") }}
