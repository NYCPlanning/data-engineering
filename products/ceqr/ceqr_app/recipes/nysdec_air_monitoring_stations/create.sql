/*
DESCRIPTION:
    1. Import nysdec_air_monitoring_stations data to EDM database using PSTDIN
    2. Create geometry from latitude and longitude
INPUTS: 
	PSTDIN >> 
    TEMP nysdec_air_monitoring_stations (
                        site_id text,
                        monitor_type text,
                        borocode integer,
                        county text,
                        site_name text,
                        air_quality_system_id text,
                        latitude double precision,
                        longitude double precision,
                        ozone text,
                        so2 text,
                        nox text,
                        co text,
                        "pm_2.5" text,
                        "cpm_2.5" text,
                        pm_10 text,
                        cpm_10 text,
                        lead text,
                        speciation text,
                        continuous_speciation text,
                        metals text,
                        toxics text,
                        carbonyls text,
                        acid_rain text,
                        pams text,
                        mercury text,
                        location text
)
OUTPUTS:
	nysdec_air_monitoring_stations.latest(
                            All fields from TEMP nysdec_air_monitoring_stations,
                            geom geometry)
    )
*/

CREATE TEMP TABLE nysdec_air_monitoring_stations (
    site_id text,
    monitor_type text,
    borocode integer,
    county text,
    site_name text,
    air_quality_system_id text,
    latitude double precision,
    longitude double precision,
    ozone text,
    so2 text,
    nox text,
    co text,
    "pm_2.5" text,
    "cpm_2.5" text,
    pm_10 text,
    cpm_10 text,
    lead text,
    speciation text,
    continuous_speciation text,
    metals text,
    toxics text,
    carbonyls text,
    acid_rain text,
    pams text,
    mercury text,
    location text
);

\COPY nysdec_air_monitoring_stations FROM PSTDIN DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS :NAME.:"VERSION" CASCADE;
SELECT 
    *,
    ST_SetSRID(ST_MakePoint(longitude,latitude),4326)::geometry(Point,4326) as geom
INTO :NAME.:"VERSION"
FROM nysdec_air_monitoring_stations;

DROP VIEW IF EXISTS :NAME.latest;
CREATE VIEW :NAME.latest AS (
    SELECT :'VERSION' as v, * 
    FROM :NAME.:"VERSION"
);