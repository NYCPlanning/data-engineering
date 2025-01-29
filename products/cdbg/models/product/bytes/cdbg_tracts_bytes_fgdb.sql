WITH cdbg_tracts AS (
    SELECT * FROM {{ ref("cdbg_tracts") }}
),

tracts AS (
    SELECT * FROM {{ ref("stg__census_tracts") }}
)

SELECT
    ROW_NUMBER() OVER () AS "OBJECTID",
    cdbg_tracts.bct2020 AS "BoroCT2020",
    tracts.nta2020 AS "NTACode",
    tracts.ntaname AS "NTAName",
    total_floor_area AS "BldgArea",
    residential_floor_area AS "ResArea",
    residential_floor_area_percentage AS "Res_pct",
    potential_lowmod_population AS "TotalPop",
    low_mod_income_population AS "LowMod_Population",
    low_mod_income_population_percentage AS "LowMod_pct",
    eligibility AS "Eligibility",
    cdbg_tracts.geom AS geometry
FROM cdbg_tracts
LEFT JOIN tracts ON cdbg_tracts.bct2020 = tracts.bct2020
