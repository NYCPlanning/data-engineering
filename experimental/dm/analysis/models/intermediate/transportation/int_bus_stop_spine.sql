-- one row per stop where riders can board on at least one route. The source lists a stop once per route and
-- direction, always at the same point.
SELECT
    bus_stop_id,
    any_value(bus_stop_name) AS bus_stop_name,
    count(DISTINCT route_id) FILTER (WHERE is_boarding) AS boarding_routes,
    any_value(geom) AS geom
FROM {{ ref('stg_mta__bus_stops') }}
GROUP BY bus_stop_id
HAVING bool_or(is_boarding)
