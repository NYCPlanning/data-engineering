SELECT
    {{ apply_text_formatting_from_seed('text_formatting__thinlion_dat') }}
FROM {{ ref('thinlion_by_field_unformatted') }}
ORDER BY borough, censustract_2020_basic, censustract_2020_suffix, dynamic_block
