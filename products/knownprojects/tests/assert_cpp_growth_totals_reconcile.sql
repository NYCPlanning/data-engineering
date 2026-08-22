-- NTAs and community districts both tile the city, so their citywide totals must
-- agree. Catches a join that matched only part of one geography's keys.

WITH nta AS (
    SELECT sum(units_2020) AS u2020, sum(units_2025) AS u2025
    FROM {{ ref('cpp_housing_growth_nta') }}
),

cd AS (
    SELECT sum(units_2020) AS u2020, sum(units_2025) AS u2025
    FROM {{ ref('cpp_housing_growth_cd') }}
)

SELECT
    nta.u2020 AS nta_units_2020,
    cd.u2020 AS cd_units_2020,
    nta.u2025 AS nta_units_2025,
    cd.u2025 AS cd_units_2025
FROM nta, cd
WHERE nta.u2020 != cd.u2020 OR nta.u2025 != cd.u2025
