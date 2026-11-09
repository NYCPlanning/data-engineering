-- Percentages run 0-100, rounded to 2 decimals (45.51 means 45.51%), and differences between
-- them are in percentage points. Null where the starting count is 0, which only happens in joint
-- interest areas.
WITH counts AS (
    SELECT
        spine.borocd,
        spine.is_joint_interest_area,
        census.population_2010_census,
        census.population_2020_census,
        census.housing_units_2010_census,
        census.housing_units_2020_census,
        coalesce(built.units_built_2010_2020, 0) AS units_built_2010_2020,
        census.housing_units_2020_census - census.housing_units_2010_census AS units_counted_change_2010_2020,
        -- worked back from the 2020 census, so the change to 2020 is only units built
        census.housing_units_2020_census - coalesce(built.units_built_2010_2020, 0) AS housing_units_2010_backcast,
        housing.housing_units_2024_end,
        kpdb.projected_net_units_2025_2034
    FROM {{ ref('int_cd_spine') }} AS spine
    LEFT JOIN {{ ref('int_cd_census_metrics') }} AS census ON spine.borocd = census.borocd
    LEFT JOIN {{ ref('int_cd_unit_completion_metrics') }} AS built ON spine.borocd = built.borocd
    LEFT JOIN {{ ref('int_cd_housing_metrics') }} AS housing ON spine.borocd = housing.borocd
    LEFT JOIN {{ ref('int_cd_kpdb_projection_metrics') }} AS kpdb ON spine.borocd = kpdb.borocd
),

pct AS (
    SELECT
        borocd,
        is_joint_interest_area,
        units_counted_change_2010_2020,
        units_counted_change_2010_2020 - units_built_2010_2020 AS units_counted_minus_built_2010_2020,
        housing_units_2010_backcast,
        round(
            100.0 * (population_2020_census - population_2010_census) / nullif(population_2010_census, 0), 2
        ) AS population_change_2010_2020_pct,
        round(100.0 * units_built_2010_2020 / nullif(housing_units_2010_backcast, 0), 2)
            AS units_built_2010_2020_pct,
        round(100.0 * units_counted_change_2010_2020 / nullif(housing_units_2010_census, 0), 2)
            AS units_counted_change_2010_2020_pct,
        round(100.0 * projected_net_units_2025_2034 / nullif(housing_units_2024_end, 0), 2)
            AS projected_net_units_2025_2034_pct
    FROM counts
)

SELECT
    *,
    round(population_change_2010_2020_pct - units_built_2010_2020_pct, 2)
        AS population_vs_housing_growth_gap_2010_2020_pts,
    -- 1 is the fastest growth of the 59 community districts. Joint interest areas aren't ranked.
    CASE WHEN NOT is_joint_interest_area THEN rank() OVER (
        PARTITION BY is_joint_interest_area ORDER BY population_change_2010_2020_pct DESC
    ) END AS population_change_2010_2020_rank,
    CASE WHEN NOT is_joint_interest_area THEN rank() OVER (
        PARTITION BY is_joint_interest_area ORDER BY units_built_2010_2020_pct DESC
    ) END AS units_built_2010_2020_rank
FROM pct
