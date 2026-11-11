SELECT * FROM {{ ref('int_subway_station_spine') }}
ORDER BY gtfs_stop_id
