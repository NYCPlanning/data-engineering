{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs']
  )
}}

-- All discrepant records across the entire CSCL project
-- Source for burndown charts and project-wide diff tracking

SELECT * FROM {{ ref('qa__diffs_thinlion_summary') }}
UNION ALL
SELECT * FROM {{ ref('qa__diffs_lion_dat') }}
