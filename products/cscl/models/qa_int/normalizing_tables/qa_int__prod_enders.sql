{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_enders_key']},
      {'columns': ['lookup_key']}
    ]
  )
}}

-- Add _enders_key to production enders output for QA comparison
-- _enders_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "enders"
) -%}

SELECT
    *,
    lookup_key AS _enders_key
FROM {{ prod_relation }}
