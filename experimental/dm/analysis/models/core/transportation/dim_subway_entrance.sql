SELECT * FROM {{ ref('int_subway_entrance_spine') }}
ORDER BY complex_id, gtfs_stop_id
