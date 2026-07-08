{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_thinfire_key']},
      {'columns': ['globalid']}
    ]
  )
}}

-- Add _thinfire_key to production thinfire_bronx output for QA comparison
-- _thinfire_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "thinfire_bronx"
) -%}

SELECT
    *,
    globalid AS _thinfire_key
FROM {{ prod_relation }}
