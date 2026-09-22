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
      old_relation=ref('qa_int__prod_saf_s_roadbed'),
      new_relation=ref('saf_s_roadbed_by_field'),
      primary_key='_saf_key',
      output_file_id='saf_s_roadbed',
      build_table_name='saf_s_roadbed_by_field',
      production_table_name='qa_int__prod_saf_s_roadbed'
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
)
SELECT
    _saf_key AS comparison_id,
    status,
    changes,
    output_file_id,
    -- Categorize based on which fields changed
    CASE
        -- Bug 006: individually reviewed by JR (Aug 2026) against source data in a spreadsheet
        -- and confirmed our new value is correct. Fingerprinted on _saf_key.
        -- See: docs/prod_bugs/006-jr-reviewed-manual-diffs-aug-2026.md
        WHEN
            status = 'modified'
            AND _saf_key = any(ARRAY['416070284412']::text[])
            THEN 'Bug 006: JR-reviewed diffs (Aug 2026)'
        -- CSCL-SAF-01: prod's lgc1 = '01' where current dcp_cscl_segment_lgc source no longer
        -- supports that classification (stale prod, not a stale load - reload-verified 26c).
        -- See: data_issues.md CSCL-SAF-01
        WHEN
            status = 'modified'
            AND change_keys <@ ARRAY['lgc1', 'lgc2', 'lgc3']::text[]
            AND 'lgc1' = any(change_keys)
            AND changes -> 'lgc1' ->> 'old' = '01'
            THEN 'CSCL-SAF-01: stale prod lgc1'
        -- If only one field changed, use that as the group name
        WHEN status = 'modified' AND array_length(change_keys, 1) = 1
            THEN change_keys[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    comparison_column,
    build_table_name,
    production_table_name,
    -- Mark as accounted for if it's a known bug/expected difference
    coalesce(
        -- Bug 006: individually reviewed by JR (Aug 2026) and confirmed correct.
        -- See: docs/prod_bugs/006-jr-reviewed-manual-diffs-aug-2026.md
        status = 'modified'
        AND _saf_key = any(ARRAY['416070284412']::text[])
        -- CSCL-SAF-01: prod's lgc1 = '01' where current source no longer supports that
        -- classification. lgc2/lgc3 may co-change (blank -> real code) alongside lgc1, but no
        -- other field. See: data_issues.md CSCL-SAF-01
        OR (
            status = 'modified'
            AND change_keys <@ ARRAY['lgc1', 'lgc2', 'lgc3']::text[]
            AND 'lgc1' = any(change_keys)
            AND changes -> 'lgc1' ->> 'old' = '01'
        ),
        FALSE
    ) AS accounted_for
FROM categorized
