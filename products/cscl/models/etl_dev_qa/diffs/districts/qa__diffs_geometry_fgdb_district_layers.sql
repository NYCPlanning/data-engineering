{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs']
  )
}}

-- Standard diff-summary shape over qa__geometry_fgdb_district_layers - see that model
-- and macros/geometry_diff_qa.sql for the pattern/caveats.
-- One model covering every layer in qa__geometry_fgdb_district_layers, output_file_id/
-- build_table_name/production_table_name derived from source_layer per row rather than
-- one wrapper file per layer. Only non-passing rows are included.
--
-- Known differences, first match wins:
--   CSCL-DISTRICTS-01: the row passes once prod's hairline slivers are removed
--     (passes_without_prod_hairlines), on any layer.
--   CSCL-DISTRICTS-06: districts whose prod polygon is stale against the current
--     AtomicPolygons (keys from inv-2cy.48; tract 1000100 is block 10001001000's
--     1,417 sq ft past every AtomicPolygon, inv-2cy.55).
--   CSCL-DISTRICTS-04: districts whose source polygon is invalid.
--   CSCL-DISTRICTS-07: nypp precincts that pass once prod's parts and holes under 100 sq
--     ft are removed, as ours drops them (passes_without_prod_small_parts).
--   CSCL-DISTRICTS-08: nypp precinct 113, which prod clipped to a different Jamaica Bay
--     shoreline than its own nybb.
WITH labeled AS (
    SELECT
        *,
        CASE
            WHEN passes_without_prod_hairlines
                THEN 'CSCL-DISTRICTS-01: prod hairline slivers'
            WHEN
                (source_layer IN ('nycc', 'nyccwi') AND key_value IN ('38', '47'))
                OR (source_layer IN ('nymc', 'nymcwi') AND key_value IN ('3|01', '3|05', '4|04'))
                OR (source_layer IN ('nyct2010wi', 'nyct2020wi') AND key_value = '1000100')
                THEN 'CSCL-DISTRICTS-06: stale prod layer'
            WHEN
                (source_layer = 'nybid' AND key_value IN ('44', '56'))
                OR (source_layer = 'nyhez' AND key_value IN ('1', '2', '4', '5', '6', 'X'))
                OR (source_layer = 'nycdwi' AND key_value IN ('484', '502'))
                THEN 'CSCL-DISTRICTS-04: invalid source polygon'
            WHEN source_layer = 'nypp' AND passes_without_prod_small_parts
                THEN 'CSCL-DISTRICTS-07: ours drops parts under 100 sq ft'
            WHEN source_layer = 'nypp' AND key_value = '113'
                THEN 'CSCL-DISTRICTS-08: prod nypp 113 shoreline'
        END AS known_difference
    FROM {{ ref('qa__geometry_fgdb_district_layers') }}
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
        'symdiff_area_pct', symdiff_area_pct,
        'hausdorff_ft', hausdorff_ft,
        'symdiff_boundary_len_ft', symdiff_boundary_len_ft,
        'symdiff_fragment_count', symdiff_fragment_count,
        'max_ring_eccentricity', max_ring_eccentricity,
        'perimeter_pct_diff', perimeter_pct_diff
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
