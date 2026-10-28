SELECT
    borocd,
    max(value) FILTER (WHERE census_year = 2010 AND pff_variable = 'pop1') AS population_2010_census,
    max(value) FILTER (WHERE census_year = 2020 AND pff_variable = 'pop1') AS population_2020_census,
    max(value) FILTER (WHERE census_year = 2010 AND pff_variable = 'hunits') AS housing_units_2010_census,
    max(value) FILTER (WHERE census_year = 2020 AND pff_variable = 'hunits') AS housing_units_2020_census
FROM {{ ref('stg_pff__decennial') }}
GROUP BY borocd
