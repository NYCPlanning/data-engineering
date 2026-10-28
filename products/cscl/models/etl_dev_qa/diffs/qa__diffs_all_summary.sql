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
--
-- Accounted-for rows split into approved (every reviewer the file's output_group
-- requires has agreed to the known differences the row cites) and awaiting approval.

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
                    THEN LEN(JSON_KEYS(changes))
                ELSE 0
            END
        ), 0) AS unaccounted_discrepant_fields,
        COUNT(*) FILTER (WHERE accounted_for AND NOT approved) AS awaiting_approval_discrepant_rows,
        COUNT(*) FILTER (WHERE approved) AS approved_discrepant_rows
    FROM {{ ref('qa__diffs_all') }}
    GROUP BY output_file_id
),

-- The LDF is compared by record count, not by keyed row (see qa__ldf_diffs /
-- qa__ldf_header_diffs) - there's no per-row "modified" concept and no per-field
-- breakdown, so each surplus record counts as one row and the field column is left null.
-- The header is one line per side: a differing header counts once, not as dev_only plus
-- prod_only.
ldf_rows AS (
    SELECT
        'ldf_dat' AS output_file_id,
        records,
        accounted_for,
        known_difference
    FROM {{ ref('qa__ldf_diffs') }}
    UNION ALL
    SELECT
        'ldf_header' AS output_file_id,
        CASE WHEN differs THEN 1 ELSE 0 END AS records,
        accounted_for,
        known_difference
    FROM {{ ref('qa__ldf_header_diffs') }}
),

ldf_approved AS (
    SELECT
        ldf_rows.*,
        COALESCE(ldf_rows.accounted_for AND signoff.approved, FALSE) AS approved
    FROM ldf_rows
    LEFT JOIN {{ ref('lion_outputs') }} AS outputs
        ON ldf_rows.output_file_id = outputs.file_id
    LEFT JOIN {{ ref('qa__known_difference_signoff') }} AS signoff
        ON
            REGEXP_EXTRACT(ldf_rows.known_difference, '^(CSCL-[A-Z]+-[0-9]{2})', 1) = signoff.id
            AND outputs.output_group = signoff.output_group
),

-- one row per LDF file even when its model has no rows
from_ldf AS (
    SELECT
        ldf_files.output_file_id,
        COALESCE(SUM(records) FILTER (WHERE accounted_for), 0) AS accounted_for_discrepant_rows,
        COALESCE(SUM(records) FILTER (WHERE NOT accounted_for), 0) AS unaccounted_discrepant_rows,
        NULL::bigint AS unaccounted_discrepant_fields,
        COALESCE(SUM(records) FILTER (WHERE accounted_for AND NOT approved), 0)
            AS awaiting_approval_discrepant_rows,
        COALESCE(SUM(records) FILTER (WHERE approved), 0) AS approved_discrepant_rows
    FROM (VALUES ('ldf_dat'), ('ldf_header')) AS ldf_files (output_file_id)
    LEFT JOIN ldf_approved ON ldf_files.output_file_id = ldf_approved.output_file_id
    GROUP BY ldf_files.output_file_id
),

counts AS (
    SELECT * FROM from_diffs_all
    UNION ALL
    SELECT * FROM from_ldf
)

SELECT
    counts.output_file_id,
    counts.accounted_for_discrepant_rows,
    counts.unaccounted_discrepant_rows,
    counts.unaccounted_discrepant_fields,
    counts.awaiting_approval_discrepant_rows,
    counts.approved_discrepant_rows,
    outputs.gr_signoff,
    outputs.gss_signoff,
    outputs.gis_signoff,
    outputs.signoff_date,
    outputs.signoff_ref
FROM counts
LEFT JOIN {{ ref('lion_outputs') }} AS outputs ON counts.output_file_id = outputs.file_id
