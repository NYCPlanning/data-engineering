{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_saf']
  )
}}

-- Compressed diff view showing one row per _saf_key with changes in jsonb
-- Includes status: modified, only_in_legacy, only_in_build

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_saf_i'),
      new_relation=ref('saf_i_by_field'),
      primary_key='_saf_key',
      output_file_id='saf_i',
      build_table_name='saf_i_by_field',
      production_table_name='qa_int__prod_saf_i'
  ) }}
),
categorized AS (
    SELECT
        _saf_key,
        status,
        changes,
        output_file_id,
        -- Get the keys from the changes jsonb
        json_keys(changes) AS change_keys,
        comparison_column,
        build_table_name,
        production_table_name
    FROM base_diffs
)
SELECT
    _saf_key AS comparison_id,
    status,
    changes,
    output_file_id,
    -- Categorize based on which fields changed
    CASE
    -- If only one field changed, use that as the group name
        WHEN status = 'modified' AND len(change_keys) = 1
            THEN change_keys[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    comparison_column,
    build_table_name,
    production_table_name,
    false AS accounted_for
FROM categorized
