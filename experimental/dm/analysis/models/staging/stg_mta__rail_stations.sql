SELECT
    railroad || '-' || code AS rail_station_id,
    railroad,
    station_name,
    branch,
    accessibility,
    geom
FROM {{ source('sources', 'mta_rail_stations') }}
