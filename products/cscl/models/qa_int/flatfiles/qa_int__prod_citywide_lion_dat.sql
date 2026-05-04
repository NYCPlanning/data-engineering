{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_lion_key']},
      {'columns': ['boroughcode', 'face_code', 'segmentid']}
    ]
  )
}}

-- Add _lion_key to production lion_dat output for QA comparison
-- _lion_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "citywide_lion_dat"
) -%}

SELECT
  *,
  boroughcode || face_code || segmentid AS _lion_key
FROM {{ prod_relation }}
