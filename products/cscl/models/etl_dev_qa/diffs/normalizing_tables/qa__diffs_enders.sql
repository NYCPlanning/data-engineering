{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_normalizing']
  )
}}

-- Compressed diff view showing one row per _enders_key with changes in jsonb
-- Includes status: modified, only_in_legacy, only_in_build

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_enders'),
      new_relation=ref('enders'),
      primary_key='_enders_key',
      output_file_id='enders',
      build_table_name='enders',
      production_table_name='qa_int__prod_enders'
  ) }}
),
categorized AS (
    SELECT
        _enders_key,
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
    _enders_key AS comparison_id,
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
