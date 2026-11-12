SELECT
    update_date::DATE AS update_date,
    project_id::INTEGER AS project_id,
    acep,
    current_budget::DOUBLE AS current_budget,
    baseline_budget::DOUBLE AS baseline_budget,
    expenditures::DOUBLE AS expenditures,
    task,
    annotations
FROM {{ source('sources', 'mta_capital_project_budgets') }}
