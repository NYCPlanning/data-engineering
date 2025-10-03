{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['segmentid']} ]
) }}

SELECT
    segmentid,
    row_type,
    'subway' AS feature_type
FROM {{ source("recipe_sources", "dcp_cscl_subway") }}
UNION
SELECT
    segmentid,
    row_type,
    'rail' AS feature_type
FROM {{ source("recipe_sources", "dcp_cscl_rail") }}
