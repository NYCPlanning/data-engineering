{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs']
  )
}}

-- All discrepant records across the entire CSCL project
-- Source for burndown charts and project-wide diff tracking

WITH unioned AS (
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
        -- saf_i is left out: it has no diffs, and its keys don't line up with prod's
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
        {%- if var('qa_gdb_lion_diffs') %}
            UNION ALL
            SELECT * FROM {{ ref('qa__diffs_gdb_lion') }}
        {%- endif %}
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
),

-- The known differences a row's labels cite: diff_group, plus subgroup, which lists
-- every cause when a row has several (qa__diffs_gdb_lion)
cited AS (
    SELECT
        *,
        REGEXP_EXTRACT_ALL(
            CONCAT_WS(' ', diff_group, subgroup), '(CSCL-[A-Z]+-[0-9]{2})'
        ) AS known_difference_ids
    FROM unioned
),

cited_ids AS (
    SELECT
        output_file_id,
        comparison_id,
        UNNEST(known_difference_ids) AS known_difference_id
    FROM cited
),

-- every cited difference agreed by every reviewer the file's output_group requires
approvals AS (
    SELECT
        cited_ids.output_file_id,
        cited_ids.comparison_id,
        BOOL_AND(COALESCE(signoff.approved, FALSE)) AS all_cited_approved
    FROM cited_ids
    LEFT JOIN {{ ref('lion_outputs') }} AS outputs
        ON cited_ids.output_file_id = outputs.file_id
    LEFT JOIN {{ ref('qa__known_difference_signoff') }} AS signoff
        ON
            cited_ids.known_difference_id = signoff.id
            AND outputs.output_group = signoff.output_group
    GROUP BY cited_ids.output_file_id, cited_ids.comparison_id
)

SELECT
    cited.*,
    COALESCE(cited.accounted_for AND approvals.all_cited_approved, FALSE) AS approved
FROM cited
LEFT JOIN approvals
    ON
        cited.output_file_id = approvals.output_file_id
        AND cited.comparison_id = approvals.comparison_id
