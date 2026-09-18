{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['_saf_key']},
      {'columns': ['boroughcode', 'face_code', 'segmentid']}
    ]
  )
}}

-- Add _saf_key to production SAF_ABCEGNPX_GENERIC output for QA comparison
-- _saf_key is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "saf_abcegnpx_generic"
) -%}

SELECT
    *,
    -- Matches the extended key in saf_abcegnpx_generic_by_field.sql - see that
    -- model for why boroughcode/face_code/segmentid alone aren't unique, and why
    -- the side_* columns are deliberately excluded from the key.
    boroughcode || '|' || face_code || '|' || segmentid || '|' || segment_seqnum
    || '|' || sos_indicator || '|' || b5sc || '|' || l_low_hn || '|' || l_high_hn
    || '|' || r_low_hn || '|' || r_high_hn || '|' || x_coord || '|' || y_coord
        AS _saf_key
FROM {{ prod_relation }}
