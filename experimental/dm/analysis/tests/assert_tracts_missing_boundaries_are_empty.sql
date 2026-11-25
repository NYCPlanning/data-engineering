-- PFF has tracts the boundary file doesn't (all-water tracts). They shouldn't hold anyone.
SELECT census.*
FROM {{ ref('int_census_tract_census_metrics') }} AS census
LEFT JOIN {{ ref('int_census_tract_spine') }} AS spine ON census.boroct2020 = spine.boroct2020
WHERE spine.boroct2020 IS NULL
    AND (
        census.population_2010_census != 0 OR census.population_2020_census != 0
        OR census.housing_units_2010_census != 0 OR census.housing_units_2020_census != 0
    )
