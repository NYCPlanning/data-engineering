{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs']
  )
}}

-- One row per output_file_id, aggregating qa__diffs_all's row-level diffs into the
-- counts consumed by poc_validation/build_diffs_report.py. Exported to CSV during
-- the build (see that script) and joined there against the lion_outputs seed and
-- the line-level file comparison.

SELECT
    output_file_id,
    COUNT(*) FILTER (WHERE accounted_for) AS accounted_for_discrepant_rows,
    COUNT(*) FILTER (WHERE NOT accounted_for) AS unaccounted_discrepant_rows,
    -- Individual changed fields (not rows) across unaccounted, modified records.
    -- only_in_build / only_in_legacy rows have no changes jsonb and so don't
    -- contribute here.
    COALESCE(SUM(
        CASE
            WHEN NOT accounted_for AND changes IS NOT NULL
                THEN (SELECT COUNT(*) FROM JSONB_OBJECT_KEYS(changes))
            ELSE 0
        END
    ), 0) AS unaccounted_discrepant_fields
FROM {{ ref('qa__diffs_all') }}
GROUP BY output_file_id
