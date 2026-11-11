-- NYC Ferry landings are the stops its ferry routes make (route_type 4), leaving out the Rockaway shuttle bus stops
-- in the same feed. The Staten Island Ferry's two terminals come from DOT.
WITH ferry_stops AS (
    SELECT DISTINCT stop_times.stop_id
    FROM {{ ref('stg_edc__nycferry_stop_times') }} AS stop_times
    INNER JOIN {{ ref('stg_edc__nycferry_trips') }} AS trips ON stop_times.trip_id = trips.trip_id
    INNER JOIN {{ ref('stg_edc__nycferry_routes') }} AS routes ON trips.route_id = routes.route_id
    WHERE routes.route_type = '4'
)

SELECT
    'NYC Ferry ' || stops.stop_id AS ferry_landing_id,
    stops.stop_name AS landing_name,
    'NYC Ferry' AS ferry_system,
    stops.geom
FROM {{ ref('stg_edc__nycferry_stops') }} AS stops
INNER JOIN ferry_stops ON stops.stop_id = ferry_stops.stop_id
UNION ALL
SELECT
    'Staten Island Ferry ' || terminal_id AS ferry_landing_id,
    terminal_name AS landing_name,
    'Staten Island Ferry' AS ferry_system,
    geom
FROM {{ ref('stg_dot__ferry_terminals') }}
