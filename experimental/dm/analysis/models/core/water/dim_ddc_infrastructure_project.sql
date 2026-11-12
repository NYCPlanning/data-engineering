SELECT * FROM {{ ref('int_ddc_infrastructure_project_spine') }}
ORDER BY ddc_project_id
