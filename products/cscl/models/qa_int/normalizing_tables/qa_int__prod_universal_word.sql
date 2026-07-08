{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_universal_word_key']},
      {'columns': ['variable_name']}
    ]
  )
}}

-- Add _universal_word_key to production universal_word output for QA comparison
-- _universal_word_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "universal_word"
) -%}

SELECT
    *,
    variable_name AS _universal_word_key
FROM {{ prod_relation }}
