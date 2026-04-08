SELECT
    {{ apply_text_formatting_from_seed('text_formatting__cdta2020_dat') }}
FROM {{ source('recipe_sources', 'dcp_cscl_cdtaequiv2020') }}
ORDER BY cdta_code
