SELECT
    globalid,
    {{ apply_text_formatting_from_seed('text_formatting__thinfire_dat') }},
    FORMAT_LION_TEXT(fire_company_type::TEXT, 1, ' ', False, True)
    || FORMAT_LION_TEXT(fire_company_number::TEXT, 3, '0', False, False) AS _thinfire_key
FROM {{ ref('thinfire_by_field_unformatted') }}
ORDER BY fire_company_type, fire_company_number
