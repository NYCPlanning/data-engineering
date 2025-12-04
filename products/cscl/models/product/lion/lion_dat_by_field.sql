{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
      {'columns': ['boroughcode', 'face_code', 'segment_seqnum']}
    ]
) }}

SELECT
    {{ apply_text_formatting_from_seed('lion_dat_formatting') }},
    source_table AS _source_table -- TODO should be dropped when pipeline is in production. Helpful in comp to prod
FROM {{ ref("int__lion") }}
WHERE include_in_geosupport_lion
