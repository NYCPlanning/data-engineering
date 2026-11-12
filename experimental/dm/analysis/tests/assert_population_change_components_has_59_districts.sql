SELECT count(*) AS districts
FROM {{ ref('population_change_components') }}
HAVING count(*) != 59
