{{ config(
    materialized = 'table'
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
        MAX(CASE WHEN boe_preferred_lgc_flag = 'Y' THEN lgc_rank END) AS boe_lgc_position,
        ARG_MAX(lgc, lgc_rank) FILTER (WHERE boe_preferred_lgc_flag = 'Y') AS boe_preferred_lgc,
        MAX(CASE WHEN lgc_rank = 1 THEN b5sc END) AS b5sc,
        MAX(CASE WHEN lgc_rank = 1 THEN b7sc END) AS preferred_b7sc
    FROM {{ ref("int__lgc") }}
    GROUP BY segmentid
),
facecode AS (
    SELECT * FROM {{ ref("stg__facecode_and_featurename_principal") }}
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
    -- BOE LGC pointer (LION L10). Correct rule (ETL spec 2.2.1): the position of the
    -- BOE-preferred LGC among the segment's LGCs. Prod's rule: 1 whenever that LGC's value
    -- is above 9 (CenterlineExtractFile.cs:1321, CSCL-LION-04). The var
    -- fix_boe_lgc_pointer picks which one LION and the LION gdb get; log__boe_lgc_pointer
    -- logs every segment where the two differ.
    lgcs_by_segment.boe_preferred_lgc,
    lgcs_by_segment.boe_lgc_position AS boe_lgc_pointer_fixed,
    CASE
        WHEN TRY_CAST(lgcs_by_segment.boe_preferred_lgc AS INT) > 9 THEN 1
        ELSE lgcs_by_segment.boe_lgc_position
    END AS boe_lgc_pointer_prod,
    {% if var('fix_boe_lgc_pointer') -%}
        lgcs_by_segment.boe_lgc_position
    {%- else -%}
        boe_lgc_pointer_prod
    {%- endif %} AS boe_lgc_pointer,
    RIGHT(lgcs_by_segment.b5sc, 5) AS five_digit_street_code,
    facecode.face_code -- TODO, not quite right for non-centerline
FROM lgcs_by_segment
LEFT JOIN facecode ON lgcs_by_segment.preferred_b7sc = facecode.b7sc
