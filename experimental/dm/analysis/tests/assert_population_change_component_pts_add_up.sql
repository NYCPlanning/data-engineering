-- The parts in points should add up to the population change (%), give or take rounding.
SELECT
    components.borocd,
    growth.population_change_2010_2020_pct
FROM {{ ref('int_cd_population_change_components') }} AS components
INNER JOIN {{ ref('int_cd_growth_metrics') }} AS growth ON components.borocd = growth.borocd
WHERE abs(
    population_change_from_units_built_2010_2020_pts
    + population_change_from_unit_count_difference_2010_2020_pts
    + population_change_from_occupancy_2010_2020_pts
    + population_change_from_household_size_2010_2020_pts
    + population_change_from_group_quarters_2010_2020_pts
    - growth.population_change_2010_2020_pct
) > 0.05
OR abs(
    population_change_from_units_built_2010_2020_pts
    + population_change_from_occupied_units_not_built_2010_2020_pts
    + population_change_from_household_size_2010_2020_pts
    + population_change_from_group_quarters_2010_2020_pts
    - growth.population_change_2010_2020_pct
) > 0.05
