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
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_saf_abcegnpx_generic') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_saf_d_roadbed') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_saf_d_generic') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_saf_ov_roadbed') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_saf_ov_generic') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_saf_s_roadbed') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_saf_s_generic') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_saf_i') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_snd') }}
    UNION ALL
    SELECT *
    FROM {{ ref('qa__diffs_sedat') }}
)

SELECT
    *,
    CURRENT_DATE AS diff_run_date
FROM unioned_diffs
