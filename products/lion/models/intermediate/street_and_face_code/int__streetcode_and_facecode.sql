{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
WITH lgc AS (
    SELECT * FROM {{ ref('int__lgc_rank') }}
    WHERE lgc_rank = 1
),
streets AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_streetname") }}
),
features AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_featurename") }}
)
SELECT
    lgc.segmentid,
    LEFT(lgc.b5sc, 1) AS boroughcode,
    lgc.b5sc,
    lgc.b7sc,
    RIGHT(lgc.b5sc, 5) AS five_digit_street_code,
    streets.facecode AS street_facecode,
    features.facecode AS feature_facecode
FROM lgc
LEFT JOIN streets ON lgc.b7sc = streets.b7sc AND streets.principal_flag = 'Y'
LEFT JOIN features ON lgc.b7sc = features.b7sc AND features.principal_flag = 'Y
