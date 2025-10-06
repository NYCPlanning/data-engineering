{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['segmentid']} ]
) }}

SELECT
    {{ dbt_utils.star(from=source("recipe_sources", "dcp_cscl_shoreline"), except=['geom']) }}
FROM {{ source("recipe_sources", "dcp_cscl_shoreline") }}
