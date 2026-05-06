SELECT
    rpl_id,
    {{ apply_text_formatting_from_seed('text_formatting__rpl') }},
    generic_segmentid || '_' || roadbed_segmentid AS _rpl_key
FROM {{ ref("int__rpl") }}
ORDER BY rpl_id ASC
