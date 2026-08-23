{{ config(severity='warn') }}

-- Warn, not error: NTA and CD citywide projections need not agree exactly.
-- Each geography allocates projects independently, and the small residual comes
-- from projects that match no boundary spatially and are placed by the fallback
-- in each model's post-hook, which can land them in a district but not an NTA.

WITH nta AS (
    SELECT sum(projected_completed_units_2026_2035) AS projected
    FROM {{ ref('cpp_housing_growth_nta') }}
),

cd AS (
    SELECT sum(projected_completed_units_2026_2035) AS projected
    FROM {{ ref('cpp_housing_growth_cd') }}
)

SELECT
    nta.projected AS nta_projected,
    cd.projected AS cd_projected,
    cd.projected - nta.projected AS difference
FROM nta, cd
WHERE nta.projected != cd.projected
