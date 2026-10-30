SELECT
    borocd,
    cdta_name,
    community_district_name,
    is_ahft_district,
    population_2010_census,
    population_2020_census,
    population_change_2010_2020_pct,
    population_change_2010_2020_rank,
    housing_units_2010_backcast,
    units_built_2010_2020,
    units_built_2010_2020_pct,
    units_built_2010_2020_rank,
    population_vs_housing_growth_gap_2010_2020_pts
FROM {{ ref('dim_community_district') }}
WHERE NOT is_joint_interest_area
ORDER BY population_vs_housing_growth_gap_2010_2020_pts DESC
