-- Splits each district's 2010-2020 population change into where it went. Population in households
-- is housing units x occupancy rate x household size, so its change splits exactly into a units,
-- an occupancy, and a household size part. A part depends on the order the factors are changed
-- in, so each one is the average over all six orders (a Shapley decomposition), which works out
-- to the change in that factor times a weight built from the other two:
--   (2 x both at 2010 + 2 x both at 2020 + each mix of 2010 and 2020) / 6
-- The units part is split again into units built and the rest of the census change in units, so
-- counting differences between the censuses don't read as construction. Group quarters is the
-- rest of the population. Each part is in people, and the parts add up to the change. The _pts
-- versions are in percentage points of 2010 population and add up to the change (%).
WITH rates AS (
    SELECT
        census.*,
        coalesce(hdb.units_built_2010_2020, 0) AS units_built,
        census.occupied_housing_units_2010_census / nullif(census.housing_units_2010_census, 0)
            AS occupancy_rate_2010,
        census.occupied_housing_units_2020_census / nullif(census.housing_units_2020_census, 0)
            AS occupancy_rate_2020,
        census.population_in_households_2010_census
        / nullif(census.occupied_housing_units_2010_census, 0) AS household_size_2010,
        census.population_in_households_2020_census
        / nullif(census.occupied_housing_units_2020_census, 0) AS household_size_2020
    FROM {{ ref('int_cd_census_metrics') }} AS census
    LEFT JOIN {{ ref('int_cd_unit_completion_metrics') }} AS hdb ON census.borocd = hdb.borocd
),

weights AS (
    SELECT
        *,
        (
            2 * occupancy_rate_2010 * household_size_2010 + 2 * occupancy_rate_2020 * household_size_2020
            + occupancy_rate_2010 * household_size_2020 + occupancy_rate_2020 * household_size_2010
        ) / 6 AS people_per_unit_weight,
        (
            2 * housing_units_2010_census * household_size_2010 + 2 * housing_units_2020_census * household_size_2020
            + housing_units_2010_census * household_size_2020 + housing_units_2020_census * household_size_2010
        ) / 6 AS occupancy_weight,
        (
            2 * housing_units_2010_census * occupancy_rate_2010 + 2 * housing_units_2020_census * occupancy_rate_2020
            + housing_units_2010_census * occupancy_rate_2020 + housing_units_2020_census * occupancy_rate_2010
        ) / 6 AS household_size_weight
    FROM rates
),

parts AS (
    SELECT
        *,
        units_built * people_per_unit_weight AS units_built_part,
        (housing_units_2020_census - housing_units_2010_census - units_built) * people_per_unit_weight
            AS unit_count_difference_part,
        (occupancy_rate_2020 - occupancy_rate_2010) * occupancy_weight AS occupancy_part,
        (household_size_2020 - household_size_2010) * household_size_weight AS household_size_part,
        group_quarters_population_2020_census - group_quarters_population_2010_census
            AS group_quarters_part
    FROM weights
),

combined AS (
    SELECT
        *,
        -- unit count difference and occupancy often offset each other (a unit one census counted
        -- and the other didn't moves both), so they're read together
        unit_count_difference_part + occupancy_part AS occupied_units_not_built_part
    FROM parts
)

SELECT
    borocd,
    round(100 * occupancy_rate_2010, 2) AS occupancy_rate_2010_census_pct,
    round(100 * occupancy_rate_2020, 2) AS occupancy_rate_2020_census_pct,
    round(household_size_2010, 2) AS average_household_size_2010_census,
    round(household_size_2020, 2) AS average_household_size_2020_census,
    round(population_in_households_2010_census / nullif(housing_units_2010_census, 0), 2)
        AS people_per_housing_unit_2010_census,
    round(population_in_households_2020_census / nullif(housing_units_2020_census, 0), 2)
        AS people_per_housing_unit_2020_census,
    population_2020_census - population_2010_census AS population_change_2010_2020,
    {%- set parts = ['units_built', 'unit_count_difference', 'occupancy', 'occupied_units_not_built', 'household_size', 'group_quarters'] %}
    {%- for part in parts %}
    round({{ part }}_part) AS population_change_from_{{ part }}_2010_2020,
    {%- endfor %}
    -- the same parts in percentage points of 2010 population, so they add up to the population
    -- change (%) and compare across districts of different sizes
    {%- for part in parts %}
    round(100 * {{ part }}_part / nullif(population_2010_census, 0), 2)
        AS population_change_from_{{ part }}_2010_2020_pts{{ "," if not loop.last }}
    {%- endfor %}
FROM combined
