SELECT * FROM {{ ref('int_school_capacity_project_spine') }}
ORDER BY start_date, project_name
