-- Each published target should be its target growth rate times its homes in 2025, and each
-- affordable share 25%, 30%, and 50% of the one above it. The report rounds to the nearest 5
-- and prints rates to 2 decimals, so allow 10 homes on the total and 5 on the shares. A row
-- here means one of the two tables was extracted wrong.
SELECT
    borocd,
    target_new_homes_2030,
    target_growth_rate_pct / 100 * total_homes_2025 AS recomputed_target_new_homes_2030
FROM {{ ref('int_cd_fhgs_features') }}
WHERE abs(target_new_homes_2030 - target_growth_rate_pct / 100 * total_homes_2025) > 10
    OR abs(target_affordable_homes_2030 - 0.25 * target_new_homes_2030) > 5
    OR abs(target_deeply_affordable_homes_2030 - 0.30 * target_affordable_homes_2030) > 5
    OR abs(target_homes_for_homeless_households_2030 - 0.50 * target_deeply_affordable_homes_2030) > 5
