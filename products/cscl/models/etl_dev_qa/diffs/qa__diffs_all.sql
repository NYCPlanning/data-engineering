{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs']
  )
}}

-- All discrepant records across the entire CSCL project
-- Source for burndown charts and project-wide diff tracking

WITH unioned_diffs AS (
    SELECT *
    FROM {{ ref('qa__diffs_thinlion_summary') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_lion_dat') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_rpl') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_saf_abcegnpx_roadbed') }}
)

SELECT
    *,
    CURRENT_DATE AS diff_run_date
FROM unioned_diffs
