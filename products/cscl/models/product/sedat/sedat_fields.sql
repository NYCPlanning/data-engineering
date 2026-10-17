SELECT
    {{ select_fields_from_seed('text_formatting__sedat') }}
FROM {{ ref("int__sedat") }}
