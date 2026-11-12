-- NTAs and districts count the same jobs from the same 2020 census total, so every date's citywide total should match.
WITH ntas AS (
    SELECT as_of_date, sum(housing_units_from_2020_census) AS units
    FROM {{ ref('mart_nta_date') }}
    GROUP BY as_of_date
),

districts AS (
    SELECT as_of_date, sum(housing_units_from_2020_census) AS units
    FROM {{ ref('mart_community_district_date') }}
    GROUP BY as_of_date
)

SELECT
    ntas.as_of_date,
    ntas.units AS nta_units,
    districts.units AS district_units
FROM ntas
FULL JOIN districts ON ntas.as_of_date = districts.as_of_date
WHERE ntas.units IS DISTINCT FROM districts.units
