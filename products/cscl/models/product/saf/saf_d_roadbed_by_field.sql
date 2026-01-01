SELECT
    {{ apply_text_formatting_from_seed('text_formatting__saf_d') }}
FROM {{ ref("int__saf_d" ) }}
WHERE roadbed
