SELECT * FROM {{ ref('int_ibx_station_spine') }}
ORDER BY ibx_station_id
