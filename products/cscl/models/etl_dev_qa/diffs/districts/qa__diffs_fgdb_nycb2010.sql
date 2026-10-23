{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_nycb2010']
  )
}}

-- Compressed diff view showing one row per bctcb2010 with changes in jsonb.
-- Includes status: modified, only_in_legacy, only_in_build.
--
-- shape_length and shape_area count as changed only beyond compare_gdb's tolerance
-- (dcpy.geospatial.gdb.compare DEFAULT_FLOAT_RTOL/ATOL, 5e-4 relative plus 5e-4
-- absolute): GDAL recomputes both on every read, so smaller differences are noise. A
-- modified row with nothing else changed is dropped.
--
-- CSCL-DISTRICTS-06: blocks where prod's block layer disagrees with the current
-- AtomicPolygons (see qa__diffs_geometry_fgdb_district_layers_cheap).

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_fgdb_nycb2010'),
      new_relation=ref('gdb_nycb2010_by_field'),
      primary_key='bctcb2010',
      output_file_id='gdb_nycb2010',
      build_table_name='gdb_nycb2010_by_field',
      production_table_name='qa_int__prod_fgdb_nycb2010'
  ) }}
),
kept AS (
    SELECT
        *,
        list_filter(
            json_keys(changes),
            k -> k NOT IN ('shape_length', 'shape_area')
            OR abs((changes -> k ->> 'old')::DOUBLE - (changes -> k ->> 'new')::DOUBLE)
            > 5e-4 + 5e-4 * abs((changes -> k ->> 'old')::DOUBLE)
        ) AS change_keys
    FROM base_diffs
),

categorized AS (
    SELECT
        bctcb2010,
        status,
        CASE
            WHEN status = 'modified'
                THEN to_json(map_from_entries(
                    list_transform(change_keys, k -> { 'key': k, 'value': changes -> k })
                ))
        END AS changes,
        output_file_id,
        change_keys,
        bctcb2010 IN (
            '10212003000', '10212004002', '10223003005', '10223013005', '20235022000',
            '20381002000', '30294001008', '30304001008', '30393001000', '30393001001',
            '40383021003', '40383021016'
        ) AS is_stale,
        comparison_column,
        build_table_name,
        production_table_name
    FROM kept
    WHERE status != 'modified' OR len(change_keys) > 0
)
SELECT
    bctcb2010 AS comparison_id,
    status,
    changes,
    output_file_id,
    -- Categorize based on which fields changed
    CASE
        WHEN is_stale
            THEN 'CSCL-DISTRICTS-06: stale prod layer'
        -- If only one field changed, use that as the group name
        WHEN status = 'modified' AND len(change_keys) = 1
            THEN change_keys[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    comparison_column,
    build_table_name,
    production_table_name,
    is_stale AS accounted_for
FROM categorized
