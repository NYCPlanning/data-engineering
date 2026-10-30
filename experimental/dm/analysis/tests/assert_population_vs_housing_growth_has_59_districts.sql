SELECT count(*) AS districts
FROM {{ ref('population_vs_housing_growth') }}
HAVING count(*) != 59
