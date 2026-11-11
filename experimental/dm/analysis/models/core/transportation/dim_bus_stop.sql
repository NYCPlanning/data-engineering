SELECT * FROM {{ ref('int_bus_stop_spine') }}
ORDER BY bus_stop_id
