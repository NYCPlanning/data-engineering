SELECT
    inputs.*,
    targets.homes_added_2021_2025,
    targets.target_new_homes AS target_new_homes_2030,
    targets.target_affordable_homes AS target_affordable_homes_2030,
    targets.target_deeply_affordable_homes AS target_deeply_affordable_homes_2030,
    targets.target_homes_for_homeless_households AS target_homes_for_homeless_households_2030,
    -- 0-100 scale like the other percentages: 200.00 means the target is twice the 2021-2025 pace
    round(100.0 * targets.target_new_homes / nullif(targets.homes_added_2021_2025, 0), 2)
        AS target_pct_of_homes_added_2021_2025
FROM {{ ref('stg_fhgs__target_inputs') }} AS inputs
LEFT JOIN {{ ref('stg_fhgs__targets') }} AS targets ON inputs.borocd = targets.borocd
