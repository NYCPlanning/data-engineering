SELECT
    gtfs_stop_id,
    station_id,
    complex_id,
    stop_name,
    constituent_station_name,
    daytime_routes,
    entrance_type,
    entry_allowed = 'YES' AS entry_allowed,
    exit_allowed = 'YES' AS exit_allowed,
    geom
FROM {{ source('sources', 'mta_subway_entrances') }}
