SELECT
    feature_id AS ibx_station_id,
    feature_name AS station_name,
    feature_label AS station_label,
    geom
FROM {{ source('sources', 'mta_ibx_stations') }}
