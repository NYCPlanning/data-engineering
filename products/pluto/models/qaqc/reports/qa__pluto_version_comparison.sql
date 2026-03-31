{{
  config(
    materialized='table',
    tags=['on_demand']
  )
}}

{% set old_relation = adapter.get_relation(
    database = "db-pluto",
    schema = "nightly_qa",
    identifier = "pluto"
) -%}

{% set dbt_relation = adapter.get_relation(
    database = "db-pluto",
    schema = "ar_dbtify_pluto_staging_models",
    identifier = "pluto"
) -%}

{%- if execute -%}
  {%- set all_columns = audit_helper._get_intersecting_columns_from_relations(old_relation, dbt_relation) -%}
  {%- set filtered_columns = all_columns | reject('in', ['id']) | list -%}
  
  WITH comparison AS (
    {{ audit_helper.compare_and_classify_relation_rows(
        a_relation = old_relation,
        b_relation = dbt_relation,
        primary_key_columns = ['bbl'], 
        columns = filtered_columns,
        sample_limit = None
    ) }}
  )
  SELECT * FROM comparison
  WHERE dbt_audit_row_status != 'identical'
{%- endif -%}
