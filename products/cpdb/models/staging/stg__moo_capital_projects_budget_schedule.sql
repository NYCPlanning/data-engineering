SELECT
    reporting_period,
    managing_agency,
    sponsor_agency,
    trim(fms_id) AS fms_id,
    pid,
    fms_project_name,
    agency_project_name,
    nullif(agency_project_description, '<blank>') AS agency_project_description,
    current_phase,
    borough,
    community_board,
    budget_line,
    ten_year_plan_category
FROM {{ source('recipe_sources', 'moo_capital_projects_budget_schedule') }}
