{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_nyfb']
  )
}}

-- Standard diff-summary shape over qa__geometry_fgdb_nyfb (the detailed per-metric
-- model - see that model and macros/geometry_diff_qa.sql for how each row is
-- computed), so nyfb's geometry QA rolls up into qa__diffs_all like every other
-- layer. Only non-passing rows are included, matching qa__diffs_fgdb_altnames'
-- convention of emitting discrepancies only, not full row-for-row parity.
--
-- Not a strict superset of the old SHAPE_Length/Area-tolerance check
-- (poc_validation/compare_gdb.py): confirmed on nyfb itself (chat log 2026-10-05) -
-- 11 of compare_gdb.py's 12 flagged rows overlap with this model's 13, but
-- FireBN 17 (28.2 ft off on a 44,599 ft perimeter - 0.063%, just past
-- compare_gdb.py's 0.05% relative tolerance) doesn't trip any of this model's
-- absolute thresholds, while FireBN 21/23 trip this model's shape-based checks
-- without moving SHAPE_Length/Area enough to trip the old one. Different,
-- overlapping methods, not one strictly containing the other - expect some rows to
-- show up in only one until both are tuned against a lot more real examples.
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
        'max_ring_eccentricity', max_ring_eccentricity
    ) AS changes,
    'gdb_nyfb' AS output_file_id,
    CASE
        WHEN array_length(failing_checks, 1) = 1 THEN failing_checks[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    'key_value' AS comparison_column,
    'gdb_nyfb' AS build_table_name,
    'fgdb_nyfb' AS production_table_name,
    false AS accounted_for
FROM {{ ref('qa__geometry_fgdb_nyfb') }}
WHERE NOT passes
