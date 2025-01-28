SELECT
    ROW_NUMBER() OVER () AS "OBJECTID",
    bct2020 AS "BoroCT2020",
    NULL AS "NTACode",
    NULL AS "NTAName",
    total_floor_area AS "BldgArea",
    residential_floor_area AS "ResArea",
    residential_floor_area_percentage AS "Res_pct",
    potential_lowmod_population AS "TotalPop",
    low_mod_income_population AS "LowMod_Population",
    low_mod_income_population_percentage AS "LoMod_pct",
    eligibility AS "Eligibility",
    geom AS geometry
FROM {{ ref("cdbg_tracts") }}
