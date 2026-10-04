{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_nypuma2010']
  )
}}

-- Standard diff-summary shape over qa__geometry_fgdb_nypuma2010 - see that model,
-- macros/geometry_diff_qa.sql, and qa__diffs_fgdb_nyfb for the pattern/caveats
-- (chat log 2026-10-04/05). Only non-passing rows are included.
SELECT
    key_value AS comparison_id,
    CASE
        WHEN only_in_build THEN 'only_in_build'
        WHEN only_in_prod THEN 'only_in_prod'
        ELSE 'modified'
    END AS status,
    jsonb_build_object(
        'symdiff_area_pct', symdiff_area_pct,
        'hausdorff_ft', hausdorff_ft,
        'symdiff_boundary_len_ft', symdiff_boundary_len_ft,
        'symdiff_fragment_count', symdiff_fragment_count,
        'max_ring_eccentricity', max_ring_eccentricity,
        'perimeter_pct_diff', perimeter_pct_diff
    ) AS changes,
    'gdb_nypuma2010' AS output_file_id,
    CASE
        WHEN array_length(failing_checks, 1) = 1 THEN failing_checks[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    'key_value' AS comparison_column,
    'gdb_nypuma2010' AS build_table_name,
    'fgdb_nypuma2010' AS production_table_name,
    false AS accounted_for
FROM {{ ref('qa__geometry_fgdb_nypuma2010') }}
WHERE NOT passes
