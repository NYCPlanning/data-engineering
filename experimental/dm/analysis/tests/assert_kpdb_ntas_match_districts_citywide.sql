-- NTAs and districts allocate the same records, so their citywide projections should match.
WITH ntas AS (
    SELECT
        sum(projected_net_units_2025_2034) AS total,
        sum(projected_net_units_2025_2029) AS first_phase,
        sum(projected_net_units_2030_2034) AS second_phase
    FROM {{ ref('dim_nta') }}
),

districts AS (
    SELECT
        sum(projected_net_units_2025_2034) AS total,
        sum(projected_net_units_2025_2029) AS first_phase,
        sum(projected_net_units_2030_2034) AS second_phase
    FROM {{ ref('dim_community_district') }}
)

SELECT *
FROM ntas, districts
WHERE ntas.total != districts.total
    -- each geography rounds its own phases, so these can drift by a few units across hundreds of rows
    OR abs(ntas.first_phase - districts.first_phase) > 100
    OR abs(ntas.second_phase - districts.second_phase) > 100
