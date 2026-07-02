SELECT
    {{ apply_text_formatting_from_seed('text_formatting__snd') }},
    b10sc AS _snd_key
FROM {{ ref("int__snd" ) }}
