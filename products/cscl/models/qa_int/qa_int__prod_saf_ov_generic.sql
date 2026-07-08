{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_saf_key']},
      {'columns': ['boroughcode', 'face_code', 'segment_seqnum']}
    ]
  )
}}

-- Add _saf_key to production SAF_OV_GENERIC output for QA comparison
-- _saf_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "saf_ov_generic"
) -%}

SELECT
    *,
    boroughcode || face_code || segment_seqnum AS _saf_key
FROM {{ prod_relation }}
