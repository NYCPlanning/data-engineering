SELECT DISTINCT
    trip_id,
    stop_id
FROM {{ source('sources', 'edc_nycferry_stop_times') }}
