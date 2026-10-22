{{
  config(
    materialized='table'
  )
}}

-- Add _rpl_key to production rpl output for QA comparison
-- _rpl_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = target.database,
    schema = "production_outputs",
    identifier = "rpl"
) -%}

SELECT
    *,
    generic_segmentid || '_' || roadbed_segmentid AS _rpl_key
FROM {{ prod_relation }}
