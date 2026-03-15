{% set old_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "thinlion_manhattan"
) -%}

{% set dbt_relation = ref('thinlion_manhattan_by_field') %}

{%- if execute -%}
  WITH production_with_atomicid AS (
    SELECT 
      *,
      borough || LPAD(censustract_2020_basic, 4, '0') || LPAD(censustract_2020_suffix, 2, '0') || dynamic_block AS atomicid
    FROM {{ old_relation }}
  )
  
  {%- set old_relation_with_atomicid = 'production_with_atomicid' -%}
  {%- set all_columns = audit_helper._get_intersecting_columns_from_relations(old_relation, dbt_relation) -%}
  {%- set filtered_columns_with_atomicid = all_columns + ['atomicid'] -%}
  
  {{ audit_helper.compare_and_classify_relation_rows(
      a_relation = old_relation_with_atomicid,
      b_relation = dbt_relation,
      primary_key_columns=['borough', 'censustract_2020_basic', 'censustract_2020_suffix', 'dynamic_block'], 
      columns = filtered_columns_with_atomicid,
      sample_limit=None
  ) }}
{%- endif -%}
