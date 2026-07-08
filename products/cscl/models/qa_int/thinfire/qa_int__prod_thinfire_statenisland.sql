{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_thinfire_key']},
      {'columns': ['globalid']}
    ]
  )
}}

-- Add _thinfire_key to production thinfire_statenisland output for QA comparison
-- _thinfire_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "thinfire_statenisland"
) -%}

SELECT
    fire_company_type,
    fire_company_number,
    fire_division,
    fire_battalion,
    borough,
    fire_company_type || fire_company_number AS globalid,
    fire_company_type || fire_company_number AS _thinfire_key
FROM {{ prod_relation }}
