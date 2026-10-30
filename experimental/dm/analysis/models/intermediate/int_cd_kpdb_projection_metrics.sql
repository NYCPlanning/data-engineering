-- KPDB's district summary only has the combined 2025-2034 projection, so the two phases are
-- summed from its records. Records are already split by district.
WITH phases AS (
    SELECT
        borocd,
        round(sum(within_5_years_in_cd)) AS projected_net_units_2025_2029,
        round(sum(from_5_to_10_years_in_cd)) AS projected_net_units_2030_2034
    FROM {{ ref('stg_kpdb__cd_records') }}
    GROUP BY borocd
)

SELECT
    summary.borocd,
    summary.projected_completed_units_2025_2034 AS projected_net_units_2025_2034,
    coalesce(phases.projected_net_units_2025_2029, 0) AS projected_net_units_2025_2029,
    coalesce(phases.projected_net_units_2030_2034, 0) AS projected_net_units_2030_2034
FROM {{ ref('stg_kpdb__cpp_housing_growth_cd') }} AS summary
LEFT JOIN phases ON summary.borocd = phases.borocd
