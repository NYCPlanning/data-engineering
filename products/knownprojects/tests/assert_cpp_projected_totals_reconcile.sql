{{ config(severity='warn') }}

-- Warn, not error: NTA and CD citywide projections do not currently agree.
-- KPDB rounds each project's units per geography row, so a project split across
-- many small geographies loses units to rounding. NTA has 262 geographies to
-- CD's 71, so NTA reads low. Pre-existing in future_units_by_*, surfaced here so
-- the two CSVs' differing citywide totals are expected rather than alarming.

WITH nta AS (
    SELECT sum(projected_completed_units_2025_2035) AS projected
    FROM {{ ref('cpp_housing_growth_nta') }}
),

cd AS (
    SELECT sum(projected_completed_units_2025_2035) AS projected
    FROM {{ ref('cpp_housing_growth_cd') }}
)

SELECT
    nta.projected AS nta_projected,
    cd.projected AS cd_projected,
    cd.projected - nta.projected AS difference
FROM nta, cd
WHERE nta.projected != cd.projected
