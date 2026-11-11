-- the budgets dataset is the only place that ties a project to its ACEP codes
SELECT DISTINCT project_id, acep
FROM {{ ref('stg_mta__capital_project_budgets') }}
