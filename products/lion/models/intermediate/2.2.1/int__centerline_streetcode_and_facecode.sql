{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
WITH centerline AS (
    SELECT * FROM {{ ref("stg__centerline") }}
),
all_lgc_pivoted AS (
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
        MAX(board_of_elections_lgc_pointer) AS board_of_elections_lgc_pointer,
        MAX(CASE WHEN lgc_rank = 1 THEN b5sc END) AS b5sc,
        MAX(CASE WHEN lgc_rank = 1 THEN b7sc END) AS preferred_b7sc
    FROM {{ ref("int__all_local_group_code_ranked") }}
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
    centerline.segmentid,
    all_lgc_pivoted.lgc1,
    all_lgc_pivoted.lgc2,
    all_lgc_pivoted.lgc3,
    all_lgc_pivoted.lgc4,
    all_lgc_pivoted.lgc5,
    all_lgc_pivoted.lgc6,
    all_lgc_pivoted.lgc7,
    all_lgc_pivoted.lgc8,
    all_lgc_pivoted.lgc9,
    all_lgc_pivoted.board_of_elections_lgc_pointer,
    RIGHT(all_lgc_pivoted.b5sc, 5) AS five_digit_street_code,
    COALESCE(principal_streetnames.facecode, principal_features.facecode) AS face_code
FROM
    centerline
LEFT JOIN all_lgc_pivoted ON centerline.segmentid = all_lgc_pivoted.segmentid
LEFT JOIN principal_streetnames ON all_lgc_pivoted.preferred_b7sc = principal_streetnames.b7sc
LEFT JOIN principal_features ON all_lgc_pivoted.preferred_b7sc = principal_features.b7sc
