-- ignoring the order of rows within each generic_segmentid grouping to focus on testing for diffs in their values
{% set old_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "rpl"
) -%}

{% set dbt_relation = ref('rpl_by_field') %}

{%- if execute -%}
  {{ audit_helper.compare_and_classify_relation_rows(
      a_relation = old_relation,
      b_relation = dbt_relation,
      primary_key_columns=['generic_segmentid', 'roadbed_segmentid'], 
      columns = None,
      sample_limit=50
  ) }}
{%- endif -%}
