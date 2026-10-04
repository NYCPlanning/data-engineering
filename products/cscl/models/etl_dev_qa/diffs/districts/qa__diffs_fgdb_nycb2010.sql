{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_nycb2010']
  )
}}

-- Compressed diff view showing one row per bctcb2010 with changes in jsonb.
-- Includes status: modified, only_in_legacy, only_in_build.
-- accounted_for is always false for now - nycb2010's known issue (CSCL-DISTRICTS-03
-- shoreline-clip geometry noise, not yet borough-bound) isn't fingerprinted to
-- specific rows yet, unlike e.g. qa__diffs_lion_dat's hardcoded bug cases.

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
categorized AS (
    SELECT
        bctcb2010,
        status,
        changes,
        output_file_id,
        -- Get the keys from the changes jsonb
        (SELECT array_agg(key) FROM jsonb_object_keys(changes) AS key) AS change_keys,
        comparison_column,
        build_table_name,
        production_table_name
    FROM base_diffs
)
SELECT
    bctcb2010 AS comparison_id,
    status,
    changes,
    output_file_id,
    -- Categorize based on which fields changed
    CASE
    -- If only one field changed, use that as the group name
        WHEN status = 'modified' AND array_length(change_keys, 1) = 1
            THEN change_keys[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    comparison_column,
    build_table_name,
    production_table_name,
    false AS accounted_for
FROM categorized
