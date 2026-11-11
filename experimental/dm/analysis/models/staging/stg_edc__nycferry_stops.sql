SELECT
    stop_id,
    stop_name,
    geom
FROM {{ source('sources', 'edc_nycferry_stops') }}
