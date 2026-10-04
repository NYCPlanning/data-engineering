{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs']
  )
}}

-- All discrepant records across the entire CSCL project
-- Source for burndown charts and project-wide diff tracking

SELECT
    *,
    CURRENT_DATE AS diff_run_date
FROM (
    SELECT * FROM {{ ref('qa__diffs_thinlion_summary') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_lion_dat') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_rpl') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_saf_abcegnpx_roadbed') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_saf_abcegnpx_generic') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_saf_d_roadbed') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_saf_d_generic') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_saf_ov_roadbed') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_saf_ov_generic') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_saf_s_roadbed') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_saf_s_generic') }}
    -- ignoring for now, as there are no diffs, and the keys are messed up vis-a-vis prod
    -- UNION ALL
    -- SELECT * FROM {{ ref('qa__diffs_saf_i') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_snd') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_sedat') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_thinfire_bronx') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_thinfire_brooklyn') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_thinfire_manhattan') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_thinfire_queens') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_thinfire_statenisland') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_enders') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_exception') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_fgdb_altnames') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_fgdb_nycb2010') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_fgdb_nyfb') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_fgdb_nycd') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_fgdb_nypuma2010') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_fgdb_nypuma2020') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_geometry_fgdb_district_layers') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_geometry_fgdb_district_layers_cheap') }}
) AS unioned_diffs
