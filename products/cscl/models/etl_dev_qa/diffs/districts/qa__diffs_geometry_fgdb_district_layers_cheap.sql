{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs']
  )
}}

-- Standard diff-summary shape over qa__geometry_fgdb_district_layers_cheap - see that
-- model, qa__diffs_geometry_fgdb_district_layers.sql (the full-check equivalent), and
-- macros/geometry_diff_qa.sql's geometry_diff_qa_cheap for the pattern/caveats (chat
-- log 2026-10-05). Only non-passing rows are included.
SELECT
    key_value AS comparison_id,
    CASE
        WHEN only_in_build THEN 'only_in_build'
        WHEN only_in_prod THEN 'only_in_prod'
        ELSE 'modified'
    END AS status,
    jsonb_build_object(
        'perimeter_pct_diff', perimeter_pct_diff,
        'area_pct_diff', area_pct_diff
    ) AS changes,
    'gdb_' || source_layer AS output_file_id,
    CASE
        WHEN array_length(failing_checks, 1) = 1 THEN failing_checks[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    'key_value' AS comparison_column,
    'gdb_' || source_layer AS build_table_name,
    'fgdb_' || source_layer AS production_table_name,
    false AS accounted_for
FROM {{ ref('qa__geometry_fgdb_district_layers_cheap') }}
WHERE NOT passes
