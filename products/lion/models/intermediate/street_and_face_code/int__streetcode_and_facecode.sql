{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
WITH lgcs_by_segment AS (
    SELECT
        segmentid,
        MAX(CASE WHEN lgc_rank = 1 THEN lgc END) AS lgc1,
        MAX(CASE WHEN lgc_rank = 2 THEN lgc END) AS lgc2,
        MAX(CASE WHEN lgc_rank = 3 THEN lgc END) AS lgc3,
        MAX(CASE WHEN lgc_rank = 4 THEN lgc END) AS lgc4,
        MAX(CASE WHEN lgc_rank = 5 THEN lgc END) AS lgc5,
        MAX(CASE WHEN lgc_rank = 6 THEN lgc END) AS lgc6,
        MAX(CASE WHEN lgc_rank = 7 THEN lgc END) AS lgc7,
        MAX(CASE WHEN lgc_rank = 8 THEN lgc END) AS lgc8,
        MAX(CASE WHEN lgc_rank = 9 THEN lgc END) AS lgc9,
        MAX(CASE WHEN boe_preferred_lgc_flag = 'Y' THEN lgc_rank END) AS boe_lgc_pointer,
        MAX(CASE WHEN lgc_rank = 1 THEN b5sc END) AS b5sc,
        MAX(CASE WHEN lgc_rank = 1 THEN b7sc END) AS preferred_b7sc
    FROM {{ ref("int__lgc") }}
    GROUP BY segmentid
),
principal_streetnames AS (
    SELECT
        b7sc,
        facecode
    FROM {{ source("recipe_sources", "dcp_cscl_streetname") }}
    WHERE principal_flag = 'Y'
),
principal_features AS (
    SELECT
        b7sc,
        facecode
    FROM {{ source("recipe_sources", "dcp_cscl_featurename") }}
    WHERE principal_flag = 'Y'
)

SELECT
    LEFT(lgcs_by_segment.b5sc, 1) AS boroughcode,
    lgcs_by_segment.segmentid,
    lgcs_by_segment.lgc1,
    lgcs_by_segment.lgc2,
    lgcs_by_segment.lgc3,
    lgcs_by_segment.lgc4,
    lgcs_by_segment.lgc5,
    lgcs_by_segment.lgc6,
    lgcs_by_segment.lgc7,
    lgcs_by_segment.lgc8,
    lgcs_by_segment.lgc9,
    lgcs_by_segment.boe_lgc_pointer,
    RIGHT(lgcs_by_segment.b5sc, 5) AS five_digit_street_code,
    COALESCE(principal_streetnames.facecode, principal_features.facecode) AS face_code
FROM lgcs_by_segment
LEFT JOIN principal_streetnames ON lgcs_by_segment.preferred_b7sc = principal_streetnames.b7sc
LEFT JOIN principal_features ON lgcs_by_segment.preferred_b7sc = principal_features.b7sc
