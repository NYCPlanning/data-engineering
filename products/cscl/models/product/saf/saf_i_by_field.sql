SELECT
    {{ apply_text_formatting_from_seed('text_formatting__saf_i') }},
    nodeid::text AS _saf_key
FROM {{ ref("int__saf_i" ) }}
