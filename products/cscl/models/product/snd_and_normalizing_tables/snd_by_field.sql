SELECT
    {{ apply_text_formatting_from_seed('text_formatting__snd') }}
FROM {{ ref("int__snd" ) }}
