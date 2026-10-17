SELECT
    {{ select_fields_from_seed('text_formatting__sedat') }}
FROM {{ ref("int__special_sedat") }}
WHERE street_name IS NOT NULL
