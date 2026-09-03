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
      old_relation=ref('qa_int__prod_saf_abcegnpx_roadbed'),
      new_relation=ref('saf_abcegnpx_roadbed_by_field'),
      primary_key='_saf_key',
      output_file_id='saf_abcegnpx_roadbed',
      build_table_name='saf_abcegnpx_roadbed_by_field',
      production_table_name='qa_int__prod_saf_abcegnpx_roadbed'
  ) }}
),
categorized AS (
    SELECT
        _saf_key,
        status,
        changes,
        output_file_id,
        -- Get the keys from the changes jsonb
        (SELECT array_agg(key) FROM jsonb_object_keys(changes) AS key) AS change_keys,
        comparison_column,
        build_table_name,
        production_table_name
    FROM base_diffs
),
diff_grouped AS (
    SELECT
        _saf_key AS comparison_id,
        status,
        changes,
        output_file_id,
        -- Categorize based on which fields changed
        CASE
            -- Bug 003: ESRI vs PostGIS point-in-polygon disagreement for SAF GNX side-AP
            -- fields. See: docs/prod_bugs/003-saf-gnx-side-ap-point-mismatch.md
            WHEN
                status = 'modified'
                AND (
                    SELECT array_agg(key ORDER BY key)
                    FROM jsonb_object_keys(changes) AS key
                ) <@ ARRAY['side_ap', 'side_borough_code', 'side_ct2020_basic',
                    'side_ct2020_suffix'
                ]::text[]
                THEN 'Bug 003: SAF GNX side-AP point mismatch'
            -- If only one field changed, use that as the group name
            WHEN status = 'modified' AND array_length(change_keys, 1) = 1
                THEN change_keys[1]
            ELSE ''
        END AS diff_group,
        '' AS subgroup,
        comparison_column,
        build_table_name,
        production_table_name
    FROM categorized
),
accounted AS (
    SELECT
        *,
        -- Mark as accounted for if it's a known bug/expected difference
        coalesce(
            diff_group = 'Bug 003: SAF GNX side-AP point mismatch', FALSE
        ) AS accounted_for
    FROM diff_grouped
)
SELECT * FROM accounted
