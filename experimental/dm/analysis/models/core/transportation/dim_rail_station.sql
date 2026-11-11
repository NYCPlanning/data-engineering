SELECT * FROM {{ ref('int_rail_station_spine') }}
ORDER BY rail_station_id
