{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_thinlion']
  )
}}

-- All discrepant records across all thinlion outputs
-- Each row represents a single discrepancy with full metadata for tracking and categorization

WITH all_diffs AS (
    SELECT * FROM {{ ref('qa__diffs_thinlion_all') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_thinlion_brooklyn') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_thinlion_bronx') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_thinlion_manhattan') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_thinlion_queens') }}
    UNION ALL
    SELECT * FROM {{ ref('qa__diffs_thinlion_statenisland') }}
),
categorized AS (
    SELECT
        atomicid,
        status,
        changes,
        output_file_id,
        -- Categorize based on which fields changed
        CASE
            -- If changes are only police_sector and/or patrol_borough
            WHEN
                status = 'modified'
                AND (
                    SELECT array_agg(key ORDER BY key)
                    FROM jsonb_object_keys(changes) AS key
                ) <@ ARRAY['patrol_borough', 'police_sector'
                ]::text []
                THEN 'police geo discrepancy'
            ELSE "group"
        END AS "group",
        '' AS subgroup,
        comparison_column,
        build_table_name,
        production_table_name,
        -- Mark as accounted for if it's a police geo discrepancy
        coalesce(
            status = 'modified'
            AND (
                SELECT array_agg(key ORDER BY key)
                FROM jsonb_object_keys(changes) AS key
            ) <@ ARRAY['patrol_borough', 'police_sector'
            ]::text [], FALSE
        ) AS accounted_for
    FROM all_diffs
)
SELECT * FROM categorized
