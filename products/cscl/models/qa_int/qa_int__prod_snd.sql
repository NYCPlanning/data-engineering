{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_snd_key']},
      {'columns': ['b10sc']}
    ]
  )
}}

-- Add _snd_key to production SND output for QA comparison
-- _snd_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "snd"
) -%}

SELECT
    *,
    b10sc AS _snd_key
FROM {{ prod_relation }}
