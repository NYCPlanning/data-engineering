{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_saf_key']},
      {'columns': ['nodeid']}
    ]
  )
}}

-- Add _saf_key to production SAF_I output for QA comparison
-- _saf_key is used as the primary key for dbt audit_helper comparisons
-- SAF I uses nodeid as the key (not boroughcode/face_code/segment_seqnum)

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "saf_i"
) -%}

SELECT
    place_name,
    nodeid::text AS nodeid,
    multiplefield,
    b7sc_intersection,
    b5sc_cross1,
    b5sc_cross2,
    saftype,
    nodeid::text AS _saf_key
FROM {{ prod_relation }}
