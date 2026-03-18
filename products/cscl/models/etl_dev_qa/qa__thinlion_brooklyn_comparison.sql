{% set old_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "thinlion_brooklyn"
) -%}

{% set dbt_relation = ref('thinlion_brooklyn_by_field') %}

{%- if execute -%}
  {%- set old_relation_with_atomicid -%}
    (SELECT 
      *,
      COALESCE(borough, '') || LPAD(COALESCE(TRIM(censustract_2020_basic::text), ''), 4, '0') || LPAD(COALESCE(TRIM(censustract_2020_suffix::text), ''), 2, '0') || COALESCE(dynamic_block, '') AS atomicid
    FROM {{ old_relation }}) AS production_with_atomicid
  {%- endset -%}
  
  {%- set all_columns = audit_helper._get_intersecting_columns_from_relations(old_relation, dbt_relation) -%}
  {%- set filtered_columns = all_columns | reject('in', ['community_development_eligibility']) | list -%}
  {%- set filtered_columns_with_atomicid = filtered_columns + ['atomicid'] -%}
  
  {{ audit_helper.compare_and_classify_relation_rows(
      a_relation = old_relation_with_atomicid,
      b_relation = dbt_relation,
      primary_key_columns=['borough', 'censustract_2020_basic', 'censustract_2020_suffix', 'dynamic_block'], 
      columns = filtered_columns_with_atomicid,
      sample_limit=None
  ) }}
{%- endif -%}
