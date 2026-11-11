-- one row per project. List columns come as Python-style lists of quoted strings, such as ['NYCT-E', 'NYCT-M'], and
-- dates as MM/DD/YYYY.
{% set lists = ['agencies', 'asset_categories', 'services', 'capital_plans', 'districts', 'initiatives'] %}
{% set dates = {
    'start_date': 'start_date',
    'goal_completion_date': 'goal_completion_date',
    'estimated_actual_completion_date': 'estimated_completion_date',
} %}
SELECT
    project_id::INTEGER AS project_id,
    title,
    stage,
    phase,
    type AS project_type,
    description,
    {%- for column in lists %}
    regexp_extract_all({{ column }}, '''([^'']*)''', 1) AS {{ column }},
    {%- endfor %}
    prime_contractor,
    contract_number,
    contract_type,
    budget_status::INTEGER AS budget_status,
    schedule_status::INTEGER AS schedule_status,
    {%- for column, name in dates.items() %}
    strptime({{ column }}, '%m/%d/%Y')::DATE AS {{ name }},
    {%- endfor %}
    goal_project_cost::DOUBLE AS goal_project_cost,
    estimated_actual_project_cost::DOUBLE AS estimated_project_cost,
    search_tags
FROM {{ source('sources', 'mta_capital_project_details') }}
