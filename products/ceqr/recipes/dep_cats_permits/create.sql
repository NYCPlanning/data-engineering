/*
DESCRIPTION:
    1. Import geocoded dep_cats_permits to EDM database using PSTDIN
    2. Filter based on ID and request type
        - exclude cancelled
        - exclude G permits
        - exclude C permits unless they are non-registrations
        - exclude CA permits unless they are not work permits and not expired
    3. Create geometry from geosupport fields
    
INPUTS: 
	PSTDIN >> 
    TEMP dep_cats_permits (
                        requestid text,
                        ...
)
OUTPUTS:
	dep_cats_permits.latest(
                            All fields from TEMP dep_cats_permits,
                            geom geometry)
    )
*/

CREATE TEMP TABLE dep_cats_permits (
    requestid text,
    applicationid text,
    requesttype text,
    ownername text,
    expiration_date date,
    make text,
    model text,
    burnermake text,
    burnermodel text,
    primaryfuel text,
    secondaryfuel text,
    quantity text,
    issue_date date,
    status text,
    premisename text,
    housenum text,
    streetname text,
    address text,
    streetname_1 text,
    streetname_2 text,
    borough text,
    geo_housenum text,
    geo_streetname text,
    geo_address text,
    geo_bbl bigint,
    geo_bin text,
    geo_latitude double precision,
    geo_longitude double precision,
    geo_x_coord double precision,
    geo_y_coord double precision,
    geo_function text
);

\COPY dep_cats_permits FROM 'output/dep_cats_permits.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS :NAME.:"VERSION" CASCADE;
SELECT 
    *,
    (CASE WHEN geo_function = 'Intersection'
        THEN ST_TRANSFORM(ST_SetSRID(ST_MakePoint(geo_x_coord,geo_y_coord),2263),4326)
        ELSE ST_SetSRID(ST_MakePoint(geo_longitude,geo_latitude),4326)
    END)::geometry(Point,4326) as geom
INTO :NAME.:"VERSION"
FROM dep_cats_permits
WHERE TRIM(status) != 'CANCELLED'
AND LEFT(applicationid, 1) != 'G'
AND (LEFT(applicationid, 1) != 'C'
    OR (requesttype != 'REGISTRATION'
        AND requesttype != 'REGISTRATION INSPECTION'
        AND requesttype != 'BOILER REGISTRATION II'))
AND (LEFT(applicationid, 2) != 'CA'
    OR requesttype != 'WORK PERMIT'
    OR TRIM(status) != 'EXPIRED');

DROP TABLE IF EXISTS :NAME."geo_rejects";
SELECT *
INTO :NAME."geo_rejects"
FROM :NAME.:"VERSION"
WHERE geom IS NULL;

DELETE
FROM :NAME.:"VERSION"
WHERE geom IS NULL;

DROP VIEW IF EXISTS :NAME.latest;
CREATE VIEW :NAME.latest AS (
    SELECT :'VERSION' as v, * 
    FROM :NAME.:"VERSION"
);