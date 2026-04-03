SELECT *
FROM {{ ref('thinfire_by_field') }}
WHERE borough = '1'
ORDER BY fire_company_type, fire_company_number
