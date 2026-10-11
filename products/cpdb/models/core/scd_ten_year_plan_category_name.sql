SELECT
    project_type,
    typc,
    name,
    first_release,
    last_release,
    first_release_order,
    last_release_order
FROM {{ ref('int__ten_year_plan_category_names') }}
