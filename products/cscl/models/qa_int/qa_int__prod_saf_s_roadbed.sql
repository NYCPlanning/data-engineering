{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_saf_key']},
      {'columns': ['boroughcode', 'face_code', 'segment_seqnum']}
    ]
  )
}}

-- Add _saf_key to production SAF_S_ROADBED output for QA comparison
-- _saf_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "saf_s_roadbed"
) -%}

SELECT
    *,
    boroughcode || face_code || segment_seqnum AS _saf_key
FROM {{ prod_relation }}
