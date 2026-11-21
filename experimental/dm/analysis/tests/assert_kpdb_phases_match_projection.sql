-- The two phases summed from KPDB's records should add up to its district projection, give or
-- take rounding each phase separately.
SELECT
    dim.borocd,
    dim.projected_net_units_2025_2029,
    dim.projected_net_units_2030_2034,
    dim.projected_net_units_2025_2034
FROM {{ ref('dim_community_district') }} AS dim
WHERE abs(
    dim.projected_net_units_2025_2029 + dim.projected_net_units_2030_2034
    - dim.projected_net_units_2025_2034
) > 1
