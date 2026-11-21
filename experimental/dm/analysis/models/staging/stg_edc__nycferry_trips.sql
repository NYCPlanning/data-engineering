SELECT
    trip_id,
    route_id
FROM {{ source('sources', 'edc_nycferry_trips') }}
