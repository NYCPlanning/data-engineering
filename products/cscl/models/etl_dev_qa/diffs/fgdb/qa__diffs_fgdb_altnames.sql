{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_fgdb']
  )
}}

-- Compressed diff view showing one row per (street, join_id) with changes in jsonb
-- Includes status: modified, only_in_legacy, only_in_build
-- NOTE: Using composite key (street|join_id) which is NOT unique (2,875 duplicates)
-- dbt_audit_helper will group duplicates and compare them as sets

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_fgdb_altnames'),
      new_relation=ref('gdb_altnames'),
      primary_key='_altnames_key',
      exclude_columns=['objectid'],
      output_file_id='fgdb_altnames',
      build_table_name='gdb_altnames',
      production_table_name='qa_int__prod_fgdb_altnames'
  ) }}
),
categorized AS (
    SELECT
        _altnames_key,
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
    _altnames_key AS comparison_id,
    status,
    changes,
    output_file_id,
    -- Categorize based on which fields changed or if orphaned
    CASE
        -- Orphan segments: only in legacy/production, non-SAF join_id
        -- These are altnames for segments that don't exist in the build or production LION
        WHEN
            status = 'only_in_legacy'
            AND right(split_part(_altnames_key, '|', 2), 1) NOT IN ('X', 'N')
            THEN 'orphan_segment'
        -- SAF segments: only in legacy because SAF not yet implemented in build
        WHEN
            status = 'only_in_legacy'
            AND right(split_part(_altnames_key, '|', 2), 1) IN ('X', 'N')
            THEN 'saf_segment'
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
