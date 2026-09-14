-- NTAs and community districts both tile the city, so the same projects add up
-- to the same citywide total either way. Units are apportioned by largest
-- remainder, so the parts sum to the whole and there is no rounding drift
-- between geographies. Any difference here means something has regressed.
{% set window = (var('cpp_latest_complete_year') | int + 1) ~ '_' ~ (var('cpp_latest_complete_year') | int + 10) %}
WITH totals AS (
    SELECT
        (
            SELECT sum(projected_completed_units_{{ window }})
            FROM {{ ref('cpp_housing_growth_nta') }}
        ) AS nta,
        (
            SELECT sum(projected_completed_units_{{ window }})
            FROM {{ ref('cpp_housing_growth_cd') }}
        ) AS cd
)

SELECT
    nta,
    cd,
    abs(cd - nta) AS difference
FROM totals
WHERE cd != nta
