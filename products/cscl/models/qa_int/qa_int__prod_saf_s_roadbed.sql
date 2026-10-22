{{
  config(
    materialized='table'
  )
}}

-- Add _saf_key to production SAF_S_ROADBED output for QA comparison
-- _saf_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = target.database,
    schema = "production_outputs",
    identifier = "saf_s_roadbed"
) -%}

SELECT
    *,
    -- Matches the extended key in saf_s_roadbed_by_field.sql - see that model for
    -- why boroughcode/face_code/segmentid alone aren't unique.
    boroughcode || '|' || face_code || '|' || segmentid || '|' || segment_seqnum
    || '|' || sos_indicator || '|' || hn || '|' || hn_suffix || '|'
    || high_alpha_hn_suffix || '|' || b5sc AS _saf_key
FROM {{ prod_relation }}
