{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_rpl_key']},
      {'columns': ['generic_segmentid', 'roadbed_segmentid']}
    ]
  )
}}

-- Add _rpl_key to production rpl output for QA comparison
-- _rpl_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "rpl"
) -%}

SELECT
  *,
  generic_segmentid || '_' || roadbed_segmentid AS _rpl_key
FROM {{ prod_relation }}
