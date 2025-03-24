/*
DESCRIPTION:
    1. Import geocoded sca_capacity_projects to EDM database using PSTDIN
    2. Create geometry from geosupport fields
    3. Move records that did not get a geoemetry to a geo_rejects table
INPUTS: 
	PSTDIN >> 
    TEMP sca_capacity_projects(uid text,
                            name text,
                            org_level text,
                            district text,
                            capacity bigint,
                            pct_ps double precision,
                            pct_is double precision,
                            pct_hs double precision,
                            guessed_pct boolean,
                            start_date date,
                            capital_plan text,
                            borough text,
                            address text,
                            geo_xy_coord text,
                            geo_x_coord double precision,
                            geo_y_coord double precision,
                            geo_from_x_coord double precision,
                            geo_from_y_coord double precision,
                            geo_to_x_coord double precision,
                            geo_to_y_coord double precision,
                            geo_function text,
                            geo_grc text,
                            geo_grc2 text,
                            geo_reason_code text,
                            geo_message text
    ) 
OUTPUTS:
	sca_capactiy_projects.latest(uid text,
                            name text,
                            org_level text,
                            district text,
                            capacity bigint,
                            pct_ps double precision,
                            pct_is double precision,
                            pct_hs double precision,
                            guessed_pct boolean,
                            start_date date,
                            capital_plan text,
                            borough text,
                            address text,
                            geo_function text,
                            geom geometry)
    ),
    sca_capactiy_projects.geo_rejects(
            Same schema as sca_capacity_projects.latest
    )
    )
*/

CREATE TEMP TABLE sca_capacity_projects (
    uid text,
    name text,
    org_level text,
    district text,
    capacity bigint,
    pct_ps double precision,
    pct_is double precision,
    pct_hs double precision,
    guessed_pct boolean,
    start_date date,
    capital_plan text,
    borough text,
    address text,
    geo_xy_coord text,
    geo_x_coord double precision,
    geo_y_coord double precision,
    geo_from_x_coord double precision,
    geo_from_y_coord double precision,
    geo_to_x_coord double precision,
    geo_to_y_coord double precision,
    geo_function text,
    geo_grc text,
    geo_grc2 text,
    geo_reason_code text,
    geo_message text
);

\COPY sca_capacity_projects FROM 'output/sca_capacity_projects.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS :NAME."geo_rejects";
SELECT *
INTO :NAME."geo_rejects"
FROM sca_capacity_projects
WHERE geo_xy_coord IS NULL and geo_x_coord IS NULL and geo_y_coord is NULL;

DROP TABLE IF EXISTS :NAME.:"VERSION" CASCADE;
SELECT 
    uid,
    name,
    org_level,
    district,
    capacity,
    pct_ps,
    pct_is,
    pct_hs,
    guessed_pct,
    start_date,
    capital_plan,
    borough,
    address,
    geo_function,
    geo_message,
    (CASE 
        -- Intersections: Create  geometry from x_coord and y_coord
        WHEN geo_function = 'Intersection'
        THEN ST_TRANSFORM(ST_SetSRID(ST_MakePoint(geo_x_coord,geo_y_coord),2263),4326)
        -- Segments: Find the middle of the segment by creating a line from to/from coords
        WHEN geo_function = 'Stretch'
        THEN ST_centroid(ST_MakeLine(
                ST_TRANSFORM(ST_SetSRID(
                    ST_MakePoint(geo_from_x_coord::NUMERIC, geo_from_y_coord::NUMERIC),2263),4326),
                ST_TRANSFORM(ST_SetSRID(
                    ST_MakePoint(geo_to_x_coord::NUMERIC, geo_to_y_coord::NUMERIC),2263),4326)))
        -- Address points: Create geometry from xy_coord
        WHEN geo_function IN ('1B','1E') 
        THEN ST_TRANSFORM(ST_SetSRID(ST_MakePoint(LEFT(geo_xy_coord, 7)::DOUBLE PRECISION,
                                            RIGHT(geo_xy_coord, 7)::DOUBLE PRECISION),2263),4326)
        ELSE NULL
    END)::geometry(Point,4326) as geom
INTO :NAME.:"VERSION"
FROM sca_capacity_projects;

DELETE
FROM :NAME.:"VERSION"
WHERE geom IS NULL;

DROP VIEW IF EXISTS :NAME.latest;
CREATE VIEW :NAME.latest AS (
    SELECT :"VERSION" as v, * 
    FROM :NAME.:"VERSION"
);