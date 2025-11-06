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
        MAX(CASE WHEN boe_preferred_lgc_flag = 'Y' THEN lgc_rank END) AS boe_lgc_pointer
    FROM {{ ref("int__lgc") }}
    GROUP BY segmentid
)
SELECT
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
FROM lgcs_by_segment
