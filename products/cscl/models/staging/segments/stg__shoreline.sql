{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['segmentid']} ]
) }}

SELECT
    *,
    '2' AS feature_type_code,
    'shoreline' AS feature_type,
    'shoreline' AS source_table
FROM {{ source("recipe_sources", "dcp_cscl_shoreline") }}
