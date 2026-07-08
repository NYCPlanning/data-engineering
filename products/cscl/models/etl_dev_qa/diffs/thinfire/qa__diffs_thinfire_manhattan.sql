{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_thinfire']
  )
}}

-- Compressed diff view showing one row per _thinfire_key with changes in jsonb
-- Includes status: modified, only_in_legacy, only_in_build

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_thinfire_manhattan'),
      new_relation=ref('thinfire_manhattan_by_field'),
      primary_key='_thinfire_key',
      output_file_id='thinfire_manhattan',
      build_table_name='thinfire_manhattan_by_field',
      production_table_name='qa_int__prod_thinfire_manhattan'
  ) }}
),
categorized AS (
    SELECT
        _thinfire_key,
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
    _thinfire_key AS comparison_id,
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
