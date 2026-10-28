-- The district rows of the 2030 targets tables should add up to the report's borough totals.
-- The report's totals differ from the sum of its rounded rows by up to 10 homes, so allow 15.
WITH districts AS (
    SELECT
        spine.boro_name AS borough,
        sum(fhgs.target_new_homes) AS target_new_homes,
        sum(fhgs.target_affordable_homes) AS target_affordable_homes,
        sum(fhgs.target_deeply_affordable_homes) AS target_deeply_affordable_homes,
        sum(fhgs.target_homes_for_homeless_households) AS target_homes_for_homeless_households
    FROM {{ ref('stg_fhgs__targets') }} AS fhgs
    INNER JOIN {{ ref('int_cd_spine') }} AS spine ON fhgs.borocd = spine.borocd
    GROUP BY spine.boro_name
)

SELECT totals.borough
FROM {{ ref('fhgs_2030_borough_totals') }} AS totals
LEFT JOIN districts ON totals.borough = districts.borough
WHERE districts.borough IS NULL
    OR abs(totals.target_new_homes - districts.target_new_homes) > 15
    OR abs(totals.target_affordable_homes - districts.target_affordable_homes) > 15
    OR abs(totals.target_deeply_affordable_homes - districts.target_deeply_affordable_homes) > 15
    OR abs(totals.target_homes_for_homeless_households - districts.target_homes_for_homeless_households) > 15
