SELECT
    district.borocd,
    district.shape_area_sqft / 43560 AS land_acres,
    census.population_2020_census / (district.shape_area_sqft / 43560) AS people_per_acre
FROM {{ ref('stg_dcp__cdboundaries') }} AS district
LEFT JOIN {{ ref('int_cd_census_metrics') }} AS census ON district.borocd = census.borocd
