SELECT
    {{ apply_text_formatting_from_seed('text_formatting__nta2020_csv') }}
FROM {{ source('recipe_sources', 'dcp_cscl_ntaequiv2020') }}
ORDER BY nta_code
