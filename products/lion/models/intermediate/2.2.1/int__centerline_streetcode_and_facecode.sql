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
        MAX(board_of_elections_lgc_pointer) AS board_of_elections_lgc_pointer
    FROM {{ ref("int__all_local_group_code_ranked") }}
    GROUP BY segmentid
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
    all_lgc_pivoted.board_of_elections_lgc_pointer
FROM
    centerline 
    LEFT JOIN all_lgc_pivoted ON centerline.segmentid = all_lgc_pivoted.segmentid
