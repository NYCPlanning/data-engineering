SELECT
    {{ apply_text_formatting_from_seed('text_formatting__saf_s') }}
FROM {{ ref("int__saf_s" ) }}
WHERE roadbed
