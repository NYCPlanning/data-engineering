SELECT
    nta.nta2020,
    nta.shape_area_sqft / 43560 AS land_acres,
    census.population_2020_census / (nta.shape_area_sqft / 43560) AS people_per_acre
FROM {{ ref('stg_dcp__nta2020') }} AS nta
LEFT JOIN {{ ref('int_nta_census_metrics') }} AS census ON nta.nta2020 = census.nta2020
