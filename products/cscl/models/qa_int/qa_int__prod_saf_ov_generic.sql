{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_saf_key']},
      {'columns': ['boroughcode', 'face_code', 'segmentid']}
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
    -- Matches the extended key in saf_ov_generic_by_field.sql - see that model for
    -- why boroughcode/face_code/segmentid alone aren't unique.
    boroughcode || face_code || segmentid || segment_seqnum || sos_indicator
    || b5sc || low_hn || low_hn_suffix || high_hn || high_hn_suffix
    || x_coord || y_coord AS _saf_key
FROM {{ prod_relation }}
