-- FHGS counts homes added 2021-2025 from the Housing Database and rounds to the nearest 5,
-- so a newer HDB version should land within a small tolerance of it.
SELECT
    fhgs.borocd,
    fhgs.homes_added_2021_2025 AS fhgs_homes_added_2021_2025,
    hdb.units_built_2021_2025
FROM {{ ref('stg_fhgs__targets') }} AS fhgs
LEFT JOIN {{ ref('int_cd_unit_completion_metrics') }} AS hdb ON fhgs.borocd = hdb.borocd
WHERE abs(fhgs.homes_added_2021_2025 - coalesce(hdb.units_built_2021_2025, 0)) > 50
