/*
DESCRIPTION:
    1. Import geocoded nysdec_title_v_facility_permits to EDM database using PSTDIN
    2. Create geometry from geosupport fields
INPUTS: 
	PSTDIN >> 
    TEMP nysdec_title_v_facility_permits (
                        facility_name text,
                        permit_id text,
                        url_to_permit_text text,
                        facility_location text,
                        address text,
                        housenum text,
                        streetname text,
                        streetname_1 text,
                        streetname_2 text,
                        facility_city text,
                        facility_state text,
                        borough text,
                        zipcode text,
                        issue_date date,
                        expiration_date date,
                        location text,
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
)
OUTPUTS:
	nysdec_title_v_facility_permits.latest(
                            All fields from TEMP nysdec_title_v_facility_permits,
                            geom geometry)
    )
*/

CREATE TEMP TABLE nysdec_title_v_facility_permits (
    facility_name text,
    permit_id text,
    url_to_permit_text text,
    facility_location text,
    address text,
    housenum text,
    streetname text,
    streetname_1 text,
    streetname_2 text,
    facility_city text,
    facility_state text,
    borough text,
    zipcode text,
    issue_date date,
    expiration_date date,
    location text,
    geo_grc text,
    geo_message text,
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


\COPY nysdec_title_v_facility_permits FROM PSTDIN DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS :NAME.:"VERSION" CASCADE;
SELECT 
    *,
    (CASE WHEN geo_function = 'Intersection'
        THEN ST_TRANSFORM(ST_SetSRID(ST_MakePoint(geo_x_coord,geo_y_coord),2263),4326)
        ELSE ST_SetSRID(ST_MakePoint(geo_longitude,geo_latitude),4326)
    END)::geometry(Point,4326) as geom
INTO :NAME.:"VERSION"
FROM nysdec_title_v_facility_permits;

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