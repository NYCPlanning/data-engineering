{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_last_word_key']},
      {'columns': ['variable_name']}
    ]
  )
}}

-- Add _last_word_key to production last_word output for QA comparison
-- _last_word_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "last_word"
) -%}

SELECT
    *,
    variable_name AS _last_word_key
FROM {{ prod_relation }}
