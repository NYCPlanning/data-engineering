-- The report's page 50 sample calculation for SI 2.
SELECT * FROM {{ ref('int_cd_fhgs_features') }}
WHERE borocd = 502
    AND (
        target_new_homes_2030 != 4605
        OR target_affordable_homes_2030 != 1150
        OR target_deeply_affordable_homes_2030 != 345
        OR target_homes_for_homeless_households_2030 != 175
    )
