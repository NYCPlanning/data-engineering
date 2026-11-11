SELECT
    stop_id AS bus_stop_id,
    stop_name AS bus_stop_name,
    route_id,
    direction_id,
    revenue_stop = '1' AS is_revenue_stop,
    boarding = '1' AS is_boarding,
    geom
FROM {{ source('sources', 'mta_bus_stops') }}
