SELECT * FROM {{ ref('int_street_improvement_project_spine') }}
ORDER BY sip_year, project_name
