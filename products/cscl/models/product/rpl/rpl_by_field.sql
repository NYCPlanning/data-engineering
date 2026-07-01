SELECT
    rpl_id,
    {{ apply_text_formatting_from_seed('text_formatting__rpl') }},
    LPAD(generic_segmentid::text, 7, '0') || '_' || LPAD(roadbed_segmentid::text, 7, '0') AS _rpl_key
FROM {{ ref("int__rpl") }}
ORDER BY rpl_id ASC
