-- Both ways of splitting the change should add up to it, give or take rounding each part.
SELECT *
FROM {{ ref('int_cd_population_change_components') }}
WHERE abs(
    population_change_from_units_built_2010_2020
    + population_change_from_unit_count_difference_2010_2020
    + population_change_from_occupancy_2010_2020
    + population_change_from_household_size_2010_2020
    + population_change_from_group_quarters_2010_2020
    - population_change_2010_2020
) > 3
OR abs(
    population_change_from_units_built_2010_2020
    + population_change_from_occupied_units_not_built_2010_2020
    + population_change_from_household_size_2010_2020
    + population_change_from_group_quarters_2010_2020
    - population_change_2010_2020
) > 3
