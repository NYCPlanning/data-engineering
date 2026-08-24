-- NTAs and community districts both tile the city, so the same projects should
-- add up to about the same citywide total either way.
--
-- They won't match exactly. Units are rounded once per project per geography,
-- and a project split across 262 NTAs rounds more often than the same project
-- across 71 districts, so a handful of units of drift is normal.
--
-- The tolerance is here to catch structural problems, not rounding. When
-- projects that matched no boundary were losing their phased measures, and
-- Hudson Square's geometry was wrong, the two disagreed by 3,795 units.

WITH totals AS (
    SELECT
        (
            SELECT sum(projected_completed_units_2026_2035)
            FROM {{ ref('cpp_housing_growth_nta') }}
        ) AS nta,
        (
            SELECT sum(projected_completed_units_2026_2035)
            FROM {{ ref('cpp_housing_growth_cd') }}
        ) AS cd
)

SELECT
    nta,
    cd,
    abs(cd - nta) AS difference
FROM totals
WHERE abs(cd - nta) > 100
