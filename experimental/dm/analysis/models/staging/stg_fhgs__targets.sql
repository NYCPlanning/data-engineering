SELECT
    {{ borocd_from_cd_code('community_district') }} AS borocd,
    homes_added_2021_2025,
    target_new_homes,
    target_affordable_homes,
    target_deeply_affordable_homes,
    target_homes_for_homeless_households
FROM {{ ref('fhgs_2030_targets') }}
