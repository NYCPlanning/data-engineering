SELECT * FROM {{ ref('int_ferry_landing_spine') }}
ORDER BY ferry_system, landing_name
