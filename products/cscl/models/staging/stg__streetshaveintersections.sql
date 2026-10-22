{{ config(
    materialized = 'table'
) }}

SELECT * FROM {{ source("recipe_sources", "dcp_cscl_streetshaveintersections") }}
