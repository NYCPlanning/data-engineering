SELECT
    {{ apply_text_formatting_from_seed('text_formatting__saf_d') }},
    boroughcode || face_code || segment_seqnum AS _saf_key
FROM {{ ref("int__saf_d" ) }}
WHERE generic
