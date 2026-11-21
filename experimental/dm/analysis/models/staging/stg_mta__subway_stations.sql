SELECT
    gtfs_stop_id,
    station_id,
    complex_id,
    stop_name,
    line,
    division,
    borough,
    daytime_routes,
    structure,
    cbd = 'true' AS in_cbd,
    CASE ada WHEN '0' THEN 'None' WHEN '1' THEN 'Full' WHEN '2' THEN 'Partial' END AS ada_accessibility,
    nullif(ada_notes, 'NaN') AS ada_notes,
    geom
FROM {{ source('sources', 'mta_subway_stations') }}
