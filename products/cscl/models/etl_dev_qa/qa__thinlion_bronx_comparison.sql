{% set old_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "thinlion_bronx"
) -%}

{% set dbt_relation = ref('thinlion_bronx_by_field') %}

{%- if execute -%}
  {{ audit_helper.compare_and_classify_relation_rows(
      a_relation = old_relation,
      b_relation = dbt_relation,
      primary_key_columns=['borough', 'censustract_2020_basic', 'censustract_2020_suffix', 'dynamic_block'], 
      columns = None,
      sample_limit=None
  ) }}
{%- endif -%}
