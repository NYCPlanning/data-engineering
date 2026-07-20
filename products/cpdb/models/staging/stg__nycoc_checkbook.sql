SELECT *
FROM {{ source('recipe_sources', 'nycoc_checkbook') }}
