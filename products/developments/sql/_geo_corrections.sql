/*
CORRECTIONS

    longitude
    latitude
    geom

*/
DROP TABLE IF EXISTS corrections_geom;

/*
Create corrections_geom
    - Translate old and new lat/lon into geometries
    - Using these geoms calculate boolean corrections criteria:
        - Distance between old and new geoms is < 10m AND
        - Old geom is not within a lot OR old geom is in water
*/
WITH
lonlat_corrections AS (
    SELECT
        a.job_number,
        coalesce(a.reason, b.reason) AS reason,
        ST_SetSRID(ST_MakePoint(a.old_lon, b.old_lat), 4326) AS old_geom,
        ST_SetSRID(ST_MakePoint(a.new_lon, b.new_lat), 4326) AS new_geom,
        a.old_lon,
        a.new_lon,
        b.old_lat,
        b.new_lat
    FROM (
        SELECT
            job_number,
            reason,
            old_value::double precision AS old_lon,
            new_value::double precision AS new_lon
        FROM _manual_corrections
        WHERE field = 'longitude'
    ) AS a LEFT JOIN (
        SELECT
            job_number,
            reason,
            old_value::double precision AS old_lat,
            new_value::double precision AS new_lat
        FROM _manual_corrections
        WHERE field = 'latitude'
    ) AS b ON a.job_number = b.job_number
),
geom_corrections AS (
    SELECT
        a.job_number,
        a.old_geom,
        a.new_geom,
        a.reason,
        a.new_lat,
        a.old_lat,
        a.new_lon,
        a.old_lon,
        b.latitude AS current_latitude,
        b.longitude AS current_longitude,
        coalesce(ST_Distance(a.new_geom, b.geom), 0) < 10 AS distance,
        get_bbl(b.geom) IS NULL AS null_bbl,
        in_water(b.geom) AS in_water
    FROM lonlat_corrections AS a
    LEFT JOIN geo_devdb AS b
        ON a.job_number = b.job_number
)
SELECT
    a.job_number,
    a.field,
    b.old_geom,
    b.new_geom,
    b.new_lat,
    b.old_lat,
    b.new_lon,
    b.old_lon,
    b.current_latitude,
    b.current_longitude,
    b.distance,
    b.null_bbl,
    b.in_water,
    b.reason,
    b.distance AND (b.null_bbl OR b.in_water) AS applicable
INTO corrections_geom
FROM _manual_corrections AS a
LEFT JOIN geom_corrections AS b
    ON a.job_number = b.job_number
WHERE a.field IN ('latitude', 'longitude');

/*
If old geom is NULL or old geom is in water and
new geom is within 10m of old geom, insert
correction into the corrections_applied table.

Append details of distance and spatial join checks to reason.
*/
INSERT INTO corrections_applied
SELECT
    job_number,
    field,
    CASE
        WHEN field = 'latitude' THEN current_latitude
        WHEN field = 'longitude' THEN current_longitude
    END AS current_value,
    CASE
        WHEN field = 'latitude' THEN old_lat
        WHEN field = 'longitude' THEN old_lon
    END AS old_value,
    CASE
        WHEN field = 'latitude' THEN new_lat
        WHEN field = 'longitude' THEN new_lon
    END AS new_value,
    reason || ' / in 10m of old geom / bbl null or in water' AS reason
FROM corrections_geom
WHERE
    applicable
    AND job_number IN (SELECT b.job_number FROM geo_devdb AS b);

/*
For all records from corrections_geom that did not
get added to corrections_applied, identify why
they didn't qualify as applicable corrections and
add them to the corrections_not_applied table.

Append disqulification criteria to reason.
*/
INSERT INTO corrections_not_applied
SELECT
    job_number,
    field,
    CASE
        WHEN field = 'latitude' THEN current_latitude
        WHEN field = 'longitude' THEN current_longitude
    END AS current_value,
    CASE
        WHEN field = 'latitude' THEN old_lat
        WHEN field = 'longitude' THEN old_lon
    END AS old_value,
    CASE
        WHEN field = 'latitude' THEN new_lat
        WHEN field = 'longitude' THEN new_lon
    END AS new_value,
    CASE
        WHEN NOT distance AND NOT (null_bbl OR in_water)
            THEN reason || ' / more than 10m of old geom / bbl not null and not in water'
        WHEN NOT distance
            THEN reason || ' / more than 10m of old geom'
        WHEN NOT (null_bbl OR in_water)
            THEN reason || ' / bbl not null and not in water'
        ELSE reason
    END AS reason
FROM corrections_geom
WHERE NOT applicable;

/*
Apply corrections where applicable
*/
UPDATE geo_devdb a
SET
    latitude = ST_Y(b.new_geom),
    longitude = ST_X(b.new_geom),
    geom = b.new_geom,
    geomsource = 'Lat/Lon DCP'
FROM corrections_geom AS b
WHERE
    a.job_number = b.job_number
    AND b.applicable;
