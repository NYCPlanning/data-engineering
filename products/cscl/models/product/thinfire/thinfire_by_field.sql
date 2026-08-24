WITH combined AS (
    SELECT
        globalid,
        {{ apply_text_formatting_from_seed('text_formatting__thinfire_dat') }}
    FROM {{ ref('thinfire_by_field_unformatted') }}
)
SELECT
    *,
    fire_company_type || fire_company_number AS _thinfire_key
FROM combined
ORDER BY fire_company_type, fire_company_number
