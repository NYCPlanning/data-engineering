SELECT
    route_id,
    route_short_name,
    route_long_name,
    route_type
FROM {{ source('sources', 'edc_nycferry_routes') }}
