SELECT
    borocd,
    cdta_name,
    community_district_name,
    is_ahft_district,
    population_2010_census,
    population_2020_census,
    population_change_2010_2020,
    population_change_2010_2020_pct,
    population_change_from_units_built_2010_2020_pts,
    population_change_from_occupied_units_not_built_2010_2020_pts,
    population_change_from_household_size_2010_2020_pts,
    population_change_from_group_quarters_2010_2020_pts,
    population_change_from_units_built_2010_2020,
    population_change_from_occupied_units_not_built_2010_2020,
    population_change_from_household_size_2010_2020,
    population_change_from_group_quarters_2010_2020,
    units_built_2010_2020,
    housing_units_2010_census,
    housing_units_2020_census,
    occupancy_rate_2010_census_pct,
    occupancy_rate_2020_census_pct,
    average_household_size_2010_census,
    average_household_size_2020_census,
    people_per_housing_unit_2010_census,
    people_per_housing_unit_2020_census
FROM {{ ref('dim_community_district') }}
WHERE NOT is_joint_interest_area
ORDER BY population_change_2010_2020_pct DESC
