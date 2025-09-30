{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
SELECT
    segmentid,
    lgc,
    ROW_NUMBER() OVER (
        PARTITION BY segmentid
        ORDER BY preferred_lgc_flag <> 'Y', lgc ASC
    ) AS lgc_rank,
    boe_preferred_lgc_flag,
    b5sc,
    b7sc
FROM {{ source("recipe_sources", "dcp_cscl_segment_lgc") }}
