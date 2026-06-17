{{ config(
    materialized = 'table'
) }}

SELECT
    globalid,
    borough,
    censustract::INTEGER AS censustract,
    neighborhood_code,
    neighborhood_name,
    puma::INTEGER AS puma
FROM {{ source("recipe_sources", "dcp_cscl_neighborhoodpumacodes") }}
ORDER BY borough, censustract
