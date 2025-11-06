{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['b7sc']},
    ]
) }}

SELECT
    b7sc,
    facecode
FROM {{ source("recipe_sources", "dcp_cscl_streetname") }}
WHERE principal_flag = 'Y'
UNION ALL
SELECT
    b7sc,
    facecode
FROM {{ source("recipe_sources", "dcp_cscl_featurename") }}
WHERE principal_flag = 'Y'
