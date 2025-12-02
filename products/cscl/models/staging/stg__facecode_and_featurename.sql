{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['b7sc']},
    ]
) }}

SELECT
    b7sc,
    lookup_key,
    facecode AS face_code,
    'street' AS feature_type
FROM {{ source("recipe_sources", "dcp_cscl_streetname") }}
WHERE principal_flag = 'Y'
UNION ALL
SELECT
    b7sc,
    lookup_key,
    facecode AS face_code,
    'feature' AS feature_type
FROM {{ source("recipe_sources", "dcp_cscl_featurename") }}
WHERE principal_flag = 'Y'
