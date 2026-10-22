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
            -- Bronx patrol borough split: production has XN/XS, we have BX (temporary until FGDB updated)
            WHEN
                status = 'modified'
                AND json_exists(changes, '$.patrol_borough')
                AND (changes -> 'patrol_borough' ->> 'new') = 'BX'
                AND (changes -> 'patrol_borough' ->> 'old') IN ('XN', 'XS')
                THEN 'CSCL-THINLION-03: Bronx patrol borough XN/XS'
            -- CSCL-THINLION-02: ESRI vs PostGIS centroid-in-polygon disagreement for police geography
            -- joins. If changes are only police_precinct and/or police_sector and/or
            -- patrol_borough and/or police_patrol_borough_command.
            -- See CSCL-THINLION-02
            WHEN
                status = 'modified'
                AND list_has_all(ARRAY[
                    'patrol_borough', 'police_patrol_borough_command', 'police_precinct',
                    'police_sector'
                ], json_keys(changes))
                THEN 'CSCL-THINLION-02: police geo centroid mismatch'
            ELSE diff_group
        END AS diff_group,
        '' AS subgroup,
        comparison_column,
        build_table_name,
        production_table_name,
        -- Mark as accounted for if it's a police geo discrepancy
        coalesce(
            -- Bronx patrol borough split
            (
                status = 'modified'
                AND json_exists(changes, '$.patrol_borough')
                AND (changes -> 'patrol_borough' ->> 'new') = 'BX'
                AND (changes -> 'patrol_borough' ->> 'old') IN ('XN', 'XS')
            )
            -- CSCL-THINLION-02: ESRI vs PostGIS centroid-in-polygon disagreement for police geography
            -- joins. See CSCL-THINLION-02
            OR (
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
