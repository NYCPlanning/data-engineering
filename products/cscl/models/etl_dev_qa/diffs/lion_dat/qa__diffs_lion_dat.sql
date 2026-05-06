{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_lion_dat']
  )
}}

-- Compressed diff view showing one row per _lion_key with changes in jsonb
-- Includes status: modified, only_in_legacy, only_in_build

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_citywide_lion_dat'),
      new_relation=ref('lion_dat_by_field'),
      primary_key='_lion_key',
      exclude_columns=['_source_table', 'filler_l38', 'filler_l45', 'filler_l64', 'filler_l65', 'filler_l89', 'filler_l199'],
      build_table_name='lion_dat_by_field',
      production_table_name='qa_int__prod_citywide_lion_dat'
  ) }}
),
with_borough AS (
    SELECT
        d.*,
        COALESCE(b.boroughcode, p.boroughcode) AS boroughcode
    FROM base_diffs AS d
    LEFT JOIN {{ ref('lion_dat_by_field') }} AS b ON d._lion_key = b._lion_key
    LEFT JOIN {{ ref('qa_int__prod_citywide_lion_dat') }} AS p ON d._lion_key = p._lion_key
),
categorized AS (
    SELECT
        _lion_key,
        status,
        changes,
        boroughcode,
        CASE boroughcode
            WHEN '1' THEN 'lion_dat_manhattan'
            WHEN '2' THEN 'lion_dat_bronx'
            WHEN '3' THEN 'lion_dat_brooklyn'
            WHEN '4' THEN 'lion_dat_queens'
            WHEN '5' THEN 'lion_dat_statenisland'
            ELSE 'lion_dat_unknown'
        END AS output_file_id,
        -- Get the keys from the changes jsonb
        (SELECT ARRAY_AGG(key) FROM JSONB_OBJECT_KEYS(changes) AS key) AS change_keys,
        comparison_column,
        build_table_name,
        production_table_name
    FROM with_borough
)
SELECT
    _lion_key AS comparison_id,
    status,
    changes,
    output_file_id,
    -- Categorize based on which fields changed
    CASE
    -- If only one field changed, use that as the group name
        WHEN status = 'modified' AND ARRAY_LENGTH(change_keys, 1) = 1
            THEN change_keys[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    comparison_column,
    build_table_name,
    production_table_name,
    -- Mark as accounted for if it's only segment_seqnum
    COALESCE(status = 'modified' AND change_keys = ARRAY['segment_seqnum']::text [], FALSE) AS accounted_for
FROM categorized
