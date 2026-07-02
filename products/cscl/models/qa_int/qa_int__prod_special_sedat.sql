{{
  config(
    enabled=false,
    materialized='table',
    indexes=[
      {'columns': ['_sedat_key']},
      {'columns': ['lionkey']}
    ]
  )
}}

-- Add _sedat_key to production SPECIAL_SEDAT output for QA comparison
-- _sedat_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "special_sedat"
) -%}

SELECT
    lionkey,
    parity,
    street_name,
    side_of_street,
    lowaddress,
    low_addr_suffix,
    highaddress,
    high_addr_suffix,
    election_district,
    assembly_district,
    b7sc,
    lionkey AS _sedat_key
FROM {{ prod_relation }}
