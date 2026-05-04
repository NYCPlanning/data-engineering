{% macro generate_diff_summary(
    old_relation,
    new_relation,
    primary_key='atomicid',
    exclude_columns=[],
    output_file_id='',
    diff_group='',
    subgroup='',
    build_table_name='',
    production_table_name=''
) %}
{#
  Generate a compressed diff summary comparing two relations

  Compares old (legacy) and new (build) relations and outputs:
  - One row per primary key
  - Status field: modified, only_in_legacy, only_in_build
  - Changes field: jsonb with old/new values for modified fields
  - Metadata fields for tracking and categorization

  Parameters:
    old_relation: dbt ref or relation for legacy/production output
    new_relation: dbt ref or relation for build output
    primary_key: column name to use as primary key (default: 'atomicid')
    exclude_columns: list of column names to exclude from comparison (default: [])
    output_file_id: identifier for the output file (e.g., 'thinlion_brooklyn')
    diff_group: categorization group (optional)
    subgroup: categorization subgroup (optional)
    build_table_name: name of the build table being compared (e.g., 'thinlion_brooklyn_by_field')
    production_table_name: name of the production table being compared (e.g., 'qa_int__prod_thinlion_brooklyn')

  Usage:
    {{ generate_diff_summary(
        old_relation=ref('qa_int__prod_thinlion_brooklyn'),
        new_relation=ref('thinlion_brooklyn_by_field'),
        primary_key='atomicid',
        output_file_id='thinlion_brooklyn',
        build_table_name='thinlion_brooklyn_by_field',
        production_table_name='qa_int__prod_thinlion_brooklyn'
    ) }}
#}

{%- if execute -%}
  {%- set all_columns = audit_helper._get_intersecting_columns_from_relations(old_relation, new_relation) -%}
  {%- set filtered_columns = all_columns | reject('equalto', primary_key) | reject('in', exclude_columns) | list + [primary_key] -%}

  WITH comparison AS (
    {{ audit_helper.compare_and_classify_relation_rows(
        a_relation = old_relation,
        b_relation = new_relation,
        primary_key_columns=[primary_key],
        columns = filtered_columns,
        sample_limit=None
    ) }}
  ),
  non_identical AS (
    SELECT * FROM comparison
    WHERE dbt_audit_row_status != 'identical'
  )
  {% set comparison_relation = 'non_identical' -%}

,
  -- Get column names for field-level comparison
  {%- set dbt_audit_columns = [primary_key, 'dbt_audit_surrogate_key', 'dbt_audit_pk_row_num',
                                'dbt_audit_in_a', 'dbt_audit_in_b', 'dbt_audit_row_status',
                                'dbt_audit_num_rows_in_status', 'dbt_audit_sample_number'] -%}
  {%- set field_columns = filtered_columns | reject('in', dbt_audit_columns) | list -%}

  -- Get rows that only exist in legacy or build (no field comparison needed)
  only_in_legacy AS (
    SELECT
      {{ primary_key }},
      'only_in_legacy' as status,
      NULL::jsonb as changes
    FROM non_identical
    WHERE dbt_audit_row_status = 'only_in_a'
  ),
  only_in_build AS (
    SELECT
      {{ primary_key }},
      'only_in_build' as status,
      NULL::jsonb as changes
    FROM non_identical
    WHERE dbt_audit_row_status = 'only_in_b'
  ),
  -- Get old and new values for modified rows
  old_values AS (
    SELECT * FROM non_identical
    WHERE dbt_audit_in_a = true
      AND dbt_audit_row_status = 'modified'
  ),
  new_values AS (
    SELECT * FROM non_identical
    WHERE dbt_audit_in_b = true
      AND dbt_audit_row_status = 'modified'
  ),
  -- Find field-level differences for modified rows
  field_diffs AS (
    {%- if field_columns | length > 0 %}
      {%- for field in field_columns %}
      SELECT
        old.{{ primary_key }},
        '{{ field }}' as field_name,
        to_jsonb(old.{{ field }}) as old_value,
        to_jsonb(new.{{ field }}) as new_value
      FROM old_values old
      INNER JOIN new_values new ON old.{{ primary_key }} = new.{{ primary_key }}
      WHERE old.{{ field }}::text IS DISTINCT FROM new.{{ field }}::text
      {{ "UNION ALL" if not loop.last else "" }}
      {%- endfor %}
    {%- else %}
      -- No fields to compare, return empty result set
      SELECT
        NULL::text as {{ primary_key }},
        NULL::text as field_name,
        NULL::jsonb as old_value,
        NULL::jsonb as new_value
      WHERE FALSE
    {%- endif %}
  ),
  -- Aggregate changes into jsonb
  modified_rows AS (
    SELECT
      {{ primary_key }},
      'modified' as status,
      jsonb_object_agg(
        field_name,
        jsonb_build_object('old', old_value, 'new', new_value)
      ) as changes
    FROM field_diffs
    GROUP BY {{ primary_key }}
  )
  -- Combine all diff types with metadata
  SELECT
    {{ primary_key }},
    status,
    changes,
    '{{ output_file_id }}' as output_file_id,
    '{{ diff_group }}' as diff_group,
    '{{ subgroup }}' as subgroup,
    '{{ primary_key }}' as comparison_column,
    '{{ build_table_name }}' as build_table_name,
    '{{ production_table_name }}' as production_table_name
  FROM only_in_legacy
  UNION ALL
  SELECT
    {{ primary_key }},
    status,
    changes,
    '{{ output_file_id }}' as output_file_id,
    '{{ diff_group }}' as diff_group,
    '{{ subgroup }}' as subgroup,
    '{{ primary_key }}' as comparison_column,
    '{{ build_table_name }}' as build_table_name,
    '{{ production_table_name }}' as production_table_name
  FROM only_in_build
  UNION ALL
  SELECT
    {{ primary_key }},
    status,
    changes,
    '{{ output_file_id }}' as output_file_id,
    '{{ diff_group }}' as diff_group,
    '{{ subgroup }}' as subgroup,
    '{{ primary_key }}' as comparison_column,
    '{{ build_table_name }}' as build_table_name,
    '{{ production_table_name }}' as production_table_name
  FROM modified_rows
{%- endif -%}

{% endmacro %}
