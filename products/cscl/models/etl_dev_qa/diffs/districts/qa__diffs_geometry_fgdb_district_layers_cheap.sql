{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs']
  )
}}

-- Standard diff-summary shape over qa__geometry_fgdb_district_layers_cheap - see that
-- model, qa__diffs_geometry_fgdb_district_layers.sql (the full-check equivalent), and
-- macros/geometry_diff_qa.sql's geometry_diff_qa_cheap for the pattern/caveats.
-- Only non-passing rows are included.
--
-- CSCL-DISTRICTS-06: blocks where prod's block layer disagrees with the current
-- AtomicPolygons (keys from inv-2cy.48 and inv-2cy.55): stale block lines, block
-- 10223003005 keyed to a tract that doesn't exist (the APs say 10223013005), water
-- block 10001001000 reaching past every AP, and AP 3990100031 in block 39901000008
-- where its source block is 0007.
WITH labeled AS (
    SELECT
        *,
        CASE
            WHEN (source_layer IN ('nycb2010', 'nycb2010wi') AND key_value IN (
                '10001001000', '10212003000', '10212004002', '10223003005', '10223013005',
                '20235022000', '20381002000', '30294001008', '30304001008', '30393001000',
                '30393001001', '39901000008', '40383021003', '40383021016'
            ))
            OR (source_layer IN ('nycb2020', 'nycb2020wi') AND key_value IN (
                '10001001000', '10212003000', '10212004000', '30393001000', '30393001001',
                '40383021003', '40383021004'
            ))
                THEN 'CSCL-DISTRICTS-06: stale prod layer'
        END AS known_difference
    FROM {{ ref('qa__geometry_fgdb_district_layers_cheap') }}
    WHERE NOT passes
)

SELECT
    key_value AS comparison_id,
    CASE
        WHEN only_in_build THEN 'only_in_build'
        WHEN only_in_prod THEN 'only_in_prod'
        ELSE 'modified'
    END AS status,
    json_object(
        'perimeter_pct_diff', perimeter_pct_diff,
        'area_pct_diff', area_pct_diff
    ) AS changes,
    'gdb_' || source_layer AS output_file_id,
    CASE
        WHEN known_difference IS NOT NULL THEN known_difference
        WHEN len(failing_checks) = 1 THEN failing_checks[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    'key_value' AS comparison_column,
    'gdb_' || source_layer AS build_table_name,
    'fgdb_' || source_layer AS production_table_name,
    known_difference IS NOT NULL AS accounted_for
FROM labeled
