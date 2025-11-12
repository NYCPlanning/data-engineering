{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['segmentid']} ]
) }}

SELECT
    segmentid,
    row_type,
    'subway' AS feature_type,
    row_type NOT IN ('1', '8') AS include_in_geosupport_lion
FROM {{ source("recipe_sources", "dcp_cscl_subway") }}
UNION
SELECT
    segmentid,
    row_type,
    'rail' AS feature_type,
    row_type NOT IN ('1', '8') AS include_in_geosupport_lion
FROM {{ source("recipe_sources", "dcp_cscl_rail") }}
