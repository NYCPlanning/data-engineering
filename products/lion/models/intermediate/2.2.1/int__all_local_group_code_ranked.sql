{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
WITH all_ranked_lgc AS (
    SELECT preferred_lgc.*
    FROM {{ ref("int__preferred_local_group_code") }} AS preferred_lgc
    UNION
    SELECT nonpreferred_lgc.*
    FROM {{ ref("int__nonpreferred_local_group_code") }} AS nonpreferred_lgc
)
SELECT
    segmentid,
    lgc,
    lgc_rank,
    CASE WHEN boe_preferred_lgc_flag = 'Y' THEN lgc_rank END AS board_of_elections_lgc_pointer
FROM all_ranked_lgc
