{{
  config(
    materialized='table'
  )
}}

-- Add _enders_key to production enders output for QA comparison
-- _enders_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = target.database,
    schema = "production_outputs",
    identifier = "enders"
) -%}

SELECT
    dat_column AS lookup_key,
    dat_column AS _enders_key
FROM {{ prod_relation }}
