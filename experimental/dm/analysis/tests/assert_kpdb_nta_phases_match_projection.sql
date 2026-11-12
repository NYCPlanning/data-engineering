-- The two phases summed from KPDB's records should add up to its NTA projection, give or take rounding each phase separately.
SELECT
    nta2020,
    projected_net_units_2025_2029,
    projected_net_units_2030_2034,
    projected_net_units_2025_2034
FROM {{ ref('dim_nta') }}
WHERE abs(projected_net_units_2025_2029 + projected_net_units_2030_2034 - projected_net_units_2025_2034) > 1
