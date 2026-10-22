{{
  config(
    materialized='table'
  )
}}

-- Add _exception_key to production exception output for QA comparison
-- _exception_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = target.database,
    schema = "production_outputs",
    identifier = "exception"
) -%}

SELECT
    dat_column AS place_name,
    dat_column AS _exception_key
FROM {{ prod_relation }}
