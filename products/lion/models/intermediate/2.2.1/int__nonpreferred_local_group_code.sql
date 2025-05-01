WITH nonpreferred AS (
    SELECT
        segmentid,
        lgc,
        boe_preferred_lgc_flag,
        b5sc,
        b7sc
    FROM
        {{ source("recipe_sources", "dcp_cscl_segment_lgc") }}
    WHERE
        preferred_lgc_flag <> 'Y'
),
nonpreferred_ranked_by_lgc AS (
    SELECT
        segmentid,
        lgc,
        RANK() OVER (
            PARTITION BY segmentid
            ORDER BY lgc ASC
        ) + 1 AS lgc_rank, -- value of 1 is reserved for preferred lgc
        boe_preferred_lgc_flag,
        b5sc,
        b7sc
    FROM nonpreferred
)

SELECT *
FROM nonpreferred_ranked_by_lgc
