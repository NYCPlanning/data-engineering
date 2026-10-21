SELECT
    {{ select_fields_from_seed('text_formatting__snd') }}
FROM {{ ref("int__snd") }}
