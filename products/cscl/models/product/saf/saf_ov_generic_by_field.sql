SELECT
    {{ apply_text_formatting_from_seed('text_formatting__saf_v') }}
FROM {{ ref("int__saf_o" ) }}
WHERE generic
UNION ALL
SELECT
    {{ apply_text_formatting_from_seed('text_formatting__saf_v') }}
FROM {{ ref("int__saf_v" ) }}
WHERE generic
