{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_snd']
  )
}}

-- Compressed diff view showing one row per _snd_key with changes in jsonb
-- Includes status: modified, only_in_legacy, only_in_build

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_snd'),
      new_relation=ref('snd_by_field'),
      primary_key='_snd_key',
      output_file_id='snd',
      build_table_name='snd_by_field',
      production_table_name='qa_int__prod_snd'
  ) }}
),
categorized AS (
    SELECT
        _snd_key,
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
-- CSCL-NORM-01: prod was built from a newer extract than ours. Rows prod has whose B10SC
-- isn't in our source at all, plus 6 features edited 2024-09 to 2026-01 whose prod values
-- our source doesn't have (inv-2cy.55).
labeled AS (
    SELECT
        c.*,
        CASE
            WHEN
                c.status = 'only_in_legacy'
                AND NOT EXISTS (
                    SELECT 1 FROM {{ ref('stg__facecode_and_featurename') }} AS f
                    WHERE f.b10sc = c._snd_key
                )
                THEN 'CSCL-NORM-01: B10SC not in our source'
            WHEN
                c.status = 'modified'
                AND c._snd_key IN (
                    '10160001010', '20148601010', '20148601020', '20148701010', '20148701020',
                    '30158210010'
                )
                THEN 'CSCL-NORM-01: feature edited since prod''s extract'
        END AS known_difference
    FROM categorized AS c
)
SELECT
    _snd_key AS comparison_id,
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
