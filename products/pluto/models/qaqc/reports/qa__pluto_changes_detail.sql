{{
  config(
    materialized='table',
    tags=['on_demand']
  )
}}
-- noqa: disable=all
-- Convert wide comparison table to narrow format with JSON change details
-- Shows only fields that changed for each BBL

{% set comparison_relation = ref('qa__pluto_version_comparison') -%}

{%- if execute -%}
  {%- set all_columns = adapter.get_columns_in_relation(comparison_relation) -%}
  {%- set exclude_columns = ['bbl', 'geom', 'dbt_audit_surrogate_key', 'dbt_audit_pk_row_num', 
                              'dbt_audit_in_a', 'dbt_audit_in_b', 'dbt_audit_row_status',
                              'dbt_audit_num_rows_in_status', 'dbt_audit_sample_number'] -%}
  {%- set field_columns = [] -%}
  {%- for col in all_columns -%}
    {%- if col.name not in exclude_columns -%}
      {%- do field_columns.append(col.name) -%}
    {%- endif -%}
  {%- endfor -%}

WITH sampled_groups AS (
  SELECT DISTINCT dbt_audit_sample_number
  FROM {{ comparison_relation }}
  WHERE dbt_audit_row_status = 'modified'
  LIMIT 1000
),
old_values AS (
  SELECT * FROM {{ comparison_relation }}
  WHERE dbt_audit_in_a = true
    AND dbt_audit_row_status = 'modified'
    AND dbt_audit_sample_number IN (SELECT dbt_audit_sample_number FROM sampled_groups)
),
new_values AS (
  SELECT * FROM {{ comparison_relation }}
  WHERE dbt_audit_in_b = true
    AND dbt_audit_row_status = 'modified'
    AND dbt_audit_sample_number IN (SELECT dbt_audit_sample_number FROM sampled_groups)
),
field_diffs AS (
  {%- for field in field_columns %}
  SELECT
    old.bbl,
    '{{ field }}' as field_name,
    to_jsonb(old.{{ field }}) as old_value,
    to_jsonb(new.{{ field }}) as new_value
  FROM old_values old
  INNER JOIN new_values new ON old.bbl = new.bbl
  WHERE old.{{ field }}::text IS DISTINCT FROM new.{{ field }}::text
  {{ "UNION ALL" if not loop.last else "" }}
  {%- endfor %}
),
aggregated_changes AS (
  SELECT
    bbl,
    jsonb_object_agg(
      field_name,
      jsonb_build_object('old', old_value, 'new', new_value)
    ) as changes
  FROM field_diffs
  GROUP BY bbl
)
SELECT
  bbl,
  'modified' as change_type,
  changes
FROM aggregated_changes
{%- endif -%}
