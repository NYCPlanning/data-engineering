{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_saf_key']},
      {'columns': ['boroughcode', 'face_code', 'segmentid']}
    ]
  )
}}

-- Add _saf_key to production SAF_D_GENERIC output for QA comparison
-- _saf_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "saf_d_generic"
) -%}

SELECT
    *,
    -- Matches the extended key in saf_d_generic_by_field.sql - see that model for
    -- why boroughcode/face_code/segmentid alone aren't unique.
    boroughcode || face_code || segmentid || segment_seqnum || sos_indicator
    || daps_b5sc || low_hn || high_hn || regular_b5sc || zipcode
    || daps_type AS _saf_key
FROM {{ prod_relation }}
