{% set old_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "thinlion_manhattan"
) -%}

{% set dbt_relation = ref('thinlion_manhattan_by_field') %}

{%- if execute -%}
  {%- set all_columns = audit_helper._get_intersecting_columns_from_relations(old_relation, dbt_relation) -%}
  {%- set filtered_columns = all_columns | reject('equalto', 'atomicid') | list -%}
  
  {{ audit_helper.compare_and_classify_relation_rows(
      a_relation = old_relation,
      b_relation = dbt_relation,
      primary_key_columns=['borough', 'censustract_2020_basic', 'censustract_2020_suffix', 'dynamic_block'], 
      columns = filtered_columns,
      sample_limit=None
  ) }}
{%- endif -%}
