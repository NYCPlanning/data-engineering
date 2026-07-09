{{
  config(
    materialized='table'
  )
}}

-- Add standardized columns to production fgdb_altnames output for QA comparison
-- Normalizes column casing to match build table format

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "fgdb_altnames"
) -%}

SELECT
    objectid,  -- Include but exclude from comparison (ESRI auto-generated)
    pdir,
    ptype,
    sname,
    stype,
    sdir,
    street,
    join_id,
    -- Composite key for comparison (street + join_id)
    -- Note: This is not unique (2,875 duplicates in production)
    -- but dbt_audit_helper will handle duplicates by grouping
    street || '|' || join_id AS _altnames_key
FROM {{ prod_relation }}
