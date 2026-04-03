SELECT
    globalid,
    {{ apply_text_formatting_from_seed('text_formatting__thinfire_dat') }}
FROM {{ ref('thinfire_by_field_unformatted') }}
ORDER BY fire_company_type, fire_company_number
