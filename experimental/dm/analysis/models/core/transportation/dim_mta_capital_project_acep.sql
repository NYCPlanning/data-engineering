SELECT * FROM {{ ref('int_mta_capital_project_acep_spine') }}
ORDER BY project_id, acep
