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
        atomicid AS comparison_id,
        status,
        changes,
        output_file_id,
        -- Categorize based on which fields changed
        CASE
            -- CSCL-THINLION-02: police geography differs on a few atomic polygons (water APs
            -- outside every precinct, and sector-only differences); cause not yet known.
            -- Matches when the only changes are police_precinct, police_sector,
            -- patrol_borough and/or police_patrol_borough_command.
            WHEN
                status = 'modified'
                AND list_has_all(ARRAY[
                    'patrol_borough', 'police_patrol_borough_command', 'police_precinct',
                    'police_sector'
                ], json_keys(changes))
                THEN 'CSCL-THINLION-02: police geography'
            ELSE diff_group
        END AS diff_group,
        '' AS subgroup,
        comparison_column,
        build_table_name,
        production_table_name,
        -- Mark as accounted for if it's a police geo discrepancy (CSCL-THINLION-02)
        coalesce(
            (
                status = 'modified'
                AND list_has_all(ARRAY[
                    'patrol_borough', 'police_patrol_borough_command', 'police_precinct',
                    'police_sector'
                ], json_keys(changes))
            ),
            FALSE
        ) AS accounted_for
    FROM all_diffs
)
SELECT * FROM categorized
