SELECT * FROM {{ ref('int_subway_service_line_spine') }}
ORDER BY service, service_line_id
