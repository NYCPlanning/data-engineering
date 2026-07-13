SELECT *
FROM {{ source('recipe_sources', 'dcp_managing_agencies_lookup') }}
