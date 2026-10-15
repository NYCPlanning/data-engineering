{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_altnames']
  )
}}

-- Compressed diff view showing one row per _altnames_key with changes in jsonb.
-- Includes status: modified, only_in_legacy, only_in_build. The key hashes the whole row,
-- so a row that differs in any field shows up once on each side, never as modified.

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_fgdb_altnames'),
      new_relation=ref('gdb_altnames_by_field'),
      primary_key='_altnames_key',
      output_file_id='gdb_altnames',
      build_table_name='gdb_altnames_by_field',
      production_table_name='qa_int__prod_fgdb_altnames'
  ) }}
),
-- CSCL-LION-09: AltNames lists names for each Join_ID in the LION feature class, so a
-- Join_ID that only one side's LION carries is a LION difference, not an AltNames one.
-- In 26c these are CommonPlaces we drop for SECURITY_LEVEL 3 that prod's LION still
-- replicates (prod-only), and CommonPlaces edited in source after prod's extract
-- (build-only).
build_lion_join_ids AS (
    SELECT DISTINCT trim("Join_ID") AS join_id FROM {{ ref('gdb_lion') }}
),
prod_lion_join_ids AS (
    SELECT DISTINCT trim(join_id) AS join_id FROM {{ ref('qa_int__prod_fgdb_lion') }}
),
diff_join_ids AS (
    SELECT DISTINCT
        _altnames_key,
        trim(join_id) AS join_id
    FROM {{ ref('gdb_altnames_by_field') }}
    UNION
    SELECT DISTINCT
        _altnames_key,
        trim(join_id) AS join_id
    FROM {{ ref('qa_int__prod_fgdb_altnames') }}
),
categorized AS (
    SELECT
        base_diffs._altnames_key,
        base_diffs.status,
        changes,
        output_file_id,
        -- Get the keys from the changes jsonb
        (SELECT array_agg(key) FROM jsonb_object_keys(changes) AS key) AS change_keys,
        comparison_column,
        build_table_name,
        production_table_name,
        (base_diffs.status = 'only_in_build' AND prod_lion.join_id IS null)
        OR (base_diffs.status = 'only_in_legacy' AND build_lion.join_id IS null) AS is_lion_join_id_gap
    FROM base_diffs
    LEFT JOIN diff_join_ids AS dj ON base_diffs._altnames_key = dj._altnames_key
    LEFT JOIN prod_lion_join_ids AS prod_lion ON dj.join_id = prod_lion.join_id
    LEFT JOIN build_lion_join_ids AS build_lion ON dj.join_id = build_lion.join_id
)
SELECT
    _altnames_key AS comparison_id,
    status,
    changes,
    output_file_id,
    -- Categorize based on which fields changed
    CASE
        WHEN is_lion_join_id_gap
            THEN 'CSCL-LION-09: Join_ID only in one side''s LION'
        -- If only one field changed, use that as the group name
        WHEN status = 'modified' AND array_length(change_keys, 1) = 1
            THEN change_keys[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    comparison_column,
    build_table_name,
    production_table_name,
    -- CSCL-LION-09: Join_ID only in one side's LION (see build_lion_join_ids above).
    coalesce(is_lion_join_id_gap, false) AS accounted_for
FROM categorized
