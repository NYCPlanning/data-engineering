{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
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
    MAX(CASE WHEN boe_preferred_lgc_flag = 'Y' THEN lgc_rank END) AS boe_lgc_pointer
FROM {{ ref("int__lgc_rank") }}
GROUP BY segmentid
