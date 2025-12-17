{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['addresspointid']},
    ]
) }}
WITH ap_lgcs AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_addresspoint_lgcs") }}
),
ranked AS (
    SELECT
        addresspointid,
        lgc,
        ROW_NUMBER() OVER (
            PARTITION BY addresspointid
            ORDER BY lgc ASC
        ) AS lgc_rank,
        boe_lgc AS boe_preferred_lgc_flag
    FROM ap_lgcs
)
SELECT
    addresspointid,
    -- primary lgc is stored in addresspoint table itself
    -- therefor, records in this table begin at lgc2
    MAX(CASE WHEN lgc_rank = 1 THEN lgc END) AS lgc2,
    MAX(CASE WHEN lgc_rank = 2 THEN lgc END) AS lgc3,
    MAX(CASE WHEN lgc_rank = 3 THEN lgc END) AS lgc4,
    MAX(CASE WHEN boe_preferred_lgc_flag = 'Y' THEN lgc_rank + 1 END) AS boe_lgc_pointer
FROM ranked
GROUP BY addresspointid
