{{ config(
    materialized = 'table',
) }}
{{ qa_compare_by_row('saf_abcegnpx_roadbed') }}
ORDER BY counts.boroughcode, counts.face_code, counts.segment_seqnum, source
