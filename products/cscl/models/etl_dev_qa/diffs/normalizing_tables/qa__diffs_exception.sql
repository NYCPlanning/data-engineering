{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_normalizing']
  )
}}

-- Compressed diff view showing one row per _exception_key with changes in jsonb
-- Includes status: modified, only_in_legacy, only_in_build

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_exception'),
      new_relation=ref('exception_by_field'),
      primary_key='_exception_key',
      output_file_id='exception',
      build_table_name='exception_by_field',
      production_table_name='qa_int__prod_exception'
  ) }}
),
categorized AS (
    SELECT
        _exception_key,
        status,
        changes,
        output_file_id,
        -- Get the keys from the changes jsonb
        json_keys(changes) AS change_keys,
        comparison_column,
        build_table_name,
        production_table_name
    FROM base_diffs
),
-- CSCL-NORM-01: prod was built from a newer extract than ours. Names prod has that our
-- source doesn't, under any flag (inv-2cy.55).
labeled AS (
    SELECT
        c.*,
        CASE
            WHEN
                c.status = 'only_in_legacy'
                AND NOT EXISTS (
                    SELECT 1 FROM {{ ref('stg__facecode_and_featurename') }} AS f
                    WHERE trim(f.place_name_sort_order) = trim(c._exception_key)
                )
                THEN 'CSCL-NORM-01: name not in our source'
        END AS known_difference
    FROM categorized AS c
)
SELECT
    _exception_key AS comparison_id,
    status,
    changes,
    output_file_id,
    -- Categorize based on which fields changed
    CASE
        WHEN known_difference IS NOT NULL THEN known_difference
        -- If only one field changed, use that as the group name
        WHEN status = 'modified' AND len(change_keys) = 1
            THEN change_keys[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    comparison_column,
    build_table_name,
    production_table_name,
    known_difference IS NOT NULL AS accounted_for
FROM labeled
