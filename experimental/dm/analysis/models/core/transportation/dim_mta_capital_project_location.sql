SELECT * FROM {{ ref('int_mta_capital_project_location_spine') }}
ORDER BY acep, location_number
