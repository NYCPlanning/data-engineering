{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['segmentid']} ]
) }}

SELECT
    *,
    CASE
        -- Non-physical census block boundary
        WHEN linetype = 3 THEN '3'
        -- Non-physical boundary other than census
        WHEN linetype IN (1, 2, 6) THEN '7'
        -- Physical boundary such as cemetery wall
        WHEN linetype IN (4, 5) THEN '8'
        -- Other non-street feature
        WHEN linetype = 7 THEN '4'
    END AS feature_type_code,
    'nonstreetfeatures' AS feature_type,
    'nonstreetfeatures' AS source_table
FROM {{ source("recipe_sources", "dcp_cscl_nonstreetfeatures") }}
