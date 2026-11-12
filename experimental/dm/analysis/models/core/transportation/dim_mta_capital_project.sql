SELECT * FROM {{ ref('int_mta_capital_project_spine') }}
ORDER BY project_id
