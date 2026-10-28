-- Percentages run 0-100, rounded to 2 decimals (45.51 means 45.51%), and differences between
-- them are in percentage points. Null where the starting count is 0, which only happens in joint
-- interest areas.
WITH pct AS (
    SELECT
        spine.borocd,
        round(
            100.0 * (census.population_2020_census - census.population_2010_census)
            / nullif(census.population_2010_census, 0),
            2
        ) AS population_change_2010_2020_pct,
        round(
            100.0 * (census.housing_units_2020_census - census.housing_units_2010_census)
            / nullif(census.housing_units_2010_census, 0),
            2
        ) AS housing_change_2010_2020_pct,
        round(
            100.0 * (housing.housing_units_2024_end - census.housing_units_2010_census)
            / nullif(census.housing_units_2010_census, 0),
            2
        ) AS housing_change_2010_2024_pct,
        round(
            100.0 * housing.projected_net_units_2025_2034
            / nullif(housing.housing_units_2024_end, 0),
            2
        ) AS projected_housing_growth_2024_2034_pct,
        round(
            100.0 * hdb.hdb_net_completed_units_2010q2_2020q1
            / nullif(census.housing_units_2010_census, 0),
            2
        ) AS hdb_housing_growth_2010_2020_pct,
        census.housing_units_2020_census - census.housing_units_2010_census
        - coalesce(hdb.hdb_net_completed_units_2010q2_2020q1, 0)
            AS census_minus_hdb_housing_change_2010_2020
    FROM {{ ref('int_cd_spine') }} AS spine
    LEFT JOIN {{ ref('int_cd_census_metrics') }} AS census ON spine.borocd = census.borocd
    LEFT JOIN {{ ref('int_cd_housing_metrics') }} AS housing ON spine.borocd = housing.borocd
    LEFT JOIN {{ ref('int_cd_hdb_completion_metrics') }} AS hdb ON spine.borocd = hdb.borocd
)

SELECT
    *,
    round(population_change_2010_2020_pct - housing_change_2010_2020_pct, 2)
        AS population_minus_housing_change_2010_2020_pts,
    round(population_change_2010_2020_pct - hdb_housing_growth_2010_2020_pct, 2)
        AS population_minus_hdb_growth_2010_2020_pts
FROM pct
