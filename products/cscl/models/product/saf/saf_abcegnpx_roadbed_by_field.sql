SELECT
    {{ apply_text_formatting_from_seed('text_formatting__saf_a') }},
    boroughcode || face_code || segment_seqnum AS _saf_key
FROM {{ ref("int__saf_abcep" ) }}
WHERE roadbed
UNION ALL
SELECT
    {{ apply_text_formatting_from_seed('text_formatting__saf_a') }},
    boroughcode || face_code || segment_seqnum AS _saf_key
FROM {{ ref("int__saf_gnx" ) }}
WHERE roadbed
