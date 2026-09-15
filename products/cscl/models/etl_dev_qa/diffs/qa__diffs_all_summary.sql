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

WITH from_diffs_all AS (
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
),

-- The LDF is compared by record count, not by keyed row (see qa__ldf_summary /
-- qa__ldf_header_diffs) - there's no per-row "modified" concept, no accounted_for
-- tracking, and no per-field breakdown, so dev_only + prod_only is folded straight
-- into unaccounted_discrepant_rows and the other two columns are left null.
from_ldf AS (
    SELECT
        'ldf_dat' AS output_file_id,
        0 AS accounted_for_discrepant_rows,
        COALESCE((
            SELECT SUM(dev_only) + SUM(prod_only)
            FROM {{ ref('qa__ldf_summary') }}
        ), 0) AS unaccounted_discrepant_rows,
        NULL::bigint AS unaccounted_discrepant_fields
    UNION ALL
    SELECT
        'ldf_header' AS output_file_id,
        0 AS accounted_for_discrepant_rows,
        COALESCE(
            (SELECT dev_only + prod_only FROM {{ ref('qa__ldf_header_diffs') }}), 0
        ) AS unaccounted_discrepant_rows,
        NULL::bigint AS unaccounted_discrepant_fields
)

SELECT * FROM from_diffs_all
UNION ALL
SELECT * FROM from_ldf
