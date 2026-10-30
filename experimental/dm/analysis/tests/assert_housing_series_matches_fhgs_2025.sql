-- FHGS counts total homes in 2025 as the 2020 census plus Housing Database completions, rounded
-- to the nearest 5, so the end of the series should land close to it.
SELECT
    mart.borocd,
    mart.housing_units_from_2020_census,
    fhgs.total_homes_2025
FROM {{ ref('mart_community_district_date') }} AS mart
INNER JOIN {{ ref('stg_fhgs__target_inputs') }} AS fhgs ON mart.borocd = fhgs.borocd
WHERE mart.as_of_date = DATE '2025-12-31'
    AND abs(mart.housing_units_from_2020_census - fhgs.total_homes_2025) > 55
