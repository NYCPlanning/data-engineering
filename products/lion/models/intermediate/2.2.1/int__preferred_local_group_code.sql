SELECT
    segmentid,
    lgc,
    1 AS lgc_rank,
    boe_preferred_lgc_flag,
    b5sc,
    b7sc
FROM
    {{ source("recipe_sources", "dcp_cscl_segment_lgc") }}
WHERE
    preferred_lgc_flag = 'Y'
