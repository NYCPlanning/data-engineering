-- one row per project location. Locations with coordinates of 0, all with park ID NA, get no geometry.
WITH locations AS (
    SELECT
        "TrackerID" AS tracker_id,
        unnest("Locations"."Location") AS location,
        generate_subscripts("Locations"."Location", 1) AS location_number
    FROM {{ source('sources', 'dpr_capitalprojects') }}
)

SELECT
    tracker_id,
    location_number,
    nullif(location.name, '') AS location_name,
    location."ParkID" AS park_id,
    CASE
        WHEN location."Latitude" <> 0 AND location."Longitude" <> 0
            THEN ST_Point(location."Longitude", location."Latitude")
    END AS geom
FROM locations
