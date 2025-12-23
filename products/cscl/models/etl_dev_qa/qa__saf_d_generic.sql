{{ config(
    materialized = 'table',
) }}
{{ qa_compare_by_row('saf_d_generic') }}
ORDER BY counts.boroughcode, counts.face_code, counts.segment_seqnum, source
