{{
  config(
    materialized='table'
  )
}}

-- Add standardized columns to production fgdb_node output for QA comparison
-- Normalizes column casing to match build table format

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "fgdb_node"
) -%}

SELECT
    nodeid,
    globalid,
    vintersect
FROM {{ prod_relation }}
