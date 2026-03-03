SELECT
    {{ apply_text_formatting_from_seed('text_formatting__rpl') }}
FROM {{ ref("int__rpl") }}
