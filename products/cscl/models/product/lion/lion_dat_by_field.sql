{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
      {'columns': ['boroughcode', 'face_code', 'segment_seqnum']},
      {'columns': ['_lion_key']}
    ]
) }}

SELECT
    {{ apply_text_formatting_from_seed('text_formatting__lion_dat') }},
    source_table AS _source_table, -- TODO should be dropped when pipeline is in production. Helpful in comp to prod
    boroughcode || face_code || LPAD(segmentid::text, 7, '0') AS _lion_key
FROM {{ ref("int__lion") }}
WHERE include_in_geosupport_lion
