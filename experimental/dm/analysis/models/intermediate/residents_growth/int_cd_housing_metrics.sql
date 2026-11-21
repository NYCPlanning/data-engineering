-- The present comes from the Housing Database series, which is newer than the version KPDB was
-- built on, and KPDB's projection is added to it, as in the notebook's housing chart.
SELECT
    series.borocd,
    series.housing_units_from_2020_census AS housing_units_2024_end,
    series.housing_units_from_2020_census + kpdb.projected_net_units_2025_2034
        AS projected_housing_units_2034_end
FROM {{ ref('int_cd_date_housing_metrics') }} AS series
LEFT JOIN {{ ref('int_cd_kpdb_projection_metrics') }} AS kpdb ON series.borocd = kpdb.borocd
WHERE series.as_of_date = DATE '2024-12-31'
