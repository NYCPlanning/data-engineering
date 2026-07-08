{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_exception_key']},
      {'columns': ['place_name']}
    ]
  )
}}

-- Add _exception_key to production exception output for QA comparison
-- _exception_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "exception"
) -%}

SELECT
    dat_column AS place_name,
    dat_column AS _exception_key
FROM {{ prod_relation }}
