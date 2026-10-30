-- Housing units at each year end from 2010 to 2025, counted from the 2020 census: units built
-- after a date before the census are subtracted, units built up to a date after it are added.
-- Same filters as int_cd_unit_completion_metrics.
WITH dates AS (
    SELECT DATE '2010-04-01' AS as_of_date
    UNION ALL
    SELECT make_date(year::integer, 12, 31) FROM range(2010, 2026) AS years (year)
    UNION ALL
    SELECT DATE '2020-04-01'
)

SELECT
    spine.borocd,
    dates.as_of_date,
    (
        census.housing_units_2020_census
        - coalesce(sum(completion.classa_net) FILTER (
            WHERE completion.date_complete > dates.as_of_date
            AND completion.date_complete <= DATE '2020-04-01'
        ), 0)
        + coalesce(sum(completion.classa_net) FILTER (
            WHERE completion.date_complete > DATE '2020-04-01'
            AND completion.date_complete <= dates.as_of_date
        ), 0)
    )::bigint AS housing_units_from_2020_census
FROM {{ ref('int_cd_spine') }} AS spine
CROSS JOIN dates
INNER JOIN {{ ref('int_cd_census_metrics') }} AS census ON spine.borocd = census.borocd
LEFT JOIN {{ ref('int_unit_completion_spine') }} AS completion
    ON
        spine.borocd = completion.borocd
        AND NOT completion.is_inactive
GROUP BY spine.borocd, dates.as_of_date, census.housing_units_2020_census
