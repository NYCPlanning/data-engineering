SELECT
    tract.boroct2020,
    tract.shape_area_sqft / 43560 AS land_acres,
    census.population_2020_census / (tract.shape_area_sqft / 43560) AS people_per_acre
FROM {{ ref('stg_dcp__ct2020') }} AS tract
LEFT JOIN {{ ref('int_census_tract_census_metrics') }} AS census ON tract.boroct2020 = census.boroct2020
