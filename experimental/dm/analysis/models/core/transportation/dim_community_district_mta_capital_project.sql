SELECT * FROM {{ ref('int_cd_mta_capital_project_spine') }}
ORDER BY borocd, acep
