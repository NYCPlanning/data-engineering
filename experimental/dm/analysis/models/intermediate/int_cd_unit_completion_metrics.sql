-- Same filters as DevDB's unit change summary (since_cen20): active jobs, by completion date.
-- The 2010-2020 window runs between the two April 1 census dates. The others are calendar years.
SELECT
    borocd,
    sum(classa_net) FILTER (
        WHERE date_complete > DATE '2010-04-01' AND date_complete <= DATE '2020-04-01'
    ) AS units_built_2010_2020,
    sum(classa_net) FILTER (WHERE complete_year BETWEEN 2015 AND 2024) AS units_built_2015_2024,
    sum(classa_net) FILTER (WHERE complete_year BETWEEN 2021 AND 2025) AS units_built_2021_2025
FROM {{ ref('int_unit_completion_spine') }}
WHERE borocd IS NOT NULL AND NOT is_inactive
GROUP BY borocd
