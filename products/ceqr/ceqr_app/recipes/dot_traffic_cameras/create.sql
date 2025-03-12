/*
DESCRIPTION:
    1. Import nysdec_air_monitoring_stations data to EDM database using PSTDIN
    2. Create geometry from latitude and longitude
INPUTS: 
	PSTDIN >> 
    TEMP tmp (
        content text,
        ...
)
OUTPUTS:
	dot_traffic_cameras.latest(
                            All fields from TEMP tmp,
                            geom geometry)
    )
*/

CREATE TEMP TABLE tmp (
    content text,
    icon text,
    id text,
    latitude double precision,
    longitude double precision,
    title text,
    url text
);


\COPY tmp FROM PSTDIN DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS :NAME.:"VERSION" CASCADE;
SELECT 
    *,
    ST_SetSRID(ST_MakePoint(longitude,latitude),4326)::geometry(Point,4326) as geom
INTO :NAME.:"VERSION"
FROM tmp;

DROP VIEW IF EXISTS :NAME.latest;
CREATE VIEW :NAME.latest AS (
    SELECT :'VERSION' as v, * 
    FROM :NAME.:"VERSION"
);