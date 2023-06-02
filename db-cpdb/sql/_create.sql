DROP TABLE IF EXISTS dcp_projecttypes_agencies CASCADE;
CREATE TABLE dcp_projecttypes_agencies (
    projecttype text,
    tycs text,
    agencyname text,
    agencyabbrev text,
    agencycode text,
    agencyacronym text,
    agency text,
    projecttypeabbrev text
);
\COPY dcp_projecttypes_agencies FROM 'data/dcp_projecttypes_agencies.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS dcp_agencylookup CASCADE;
CREATE TABLE dcp_agencylookup (
    facdb_level text,
    facdb_agencyname_revised text,
    facdb_agencyname text,
    facdb_agencyabbrev text,
    cape_agencycode text,
    cape_agencyacronym text,
    cape_agency text
);
\COPY dcp_agencylookup FROM 'data/agencylookup.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS dcp_json CASCADE;
CREATE TABLE dcp_json (
    maprojid text, 
    geom text
);
\COPY dcp_json FROM 'data/dcp_json.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS cpdb_badgeoms CASCADE;
CREATE TABLE cpdb_badgeoms (
    maprojid text
);
\COPY cpdb_badgeoms FROM 'data/cpdb_geomsremove.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS dcp_id_bin_map CASCADE;
CREATE TABLE dcp_id_bin_map (
    maprojid text, 
    bin text
);
\COPY dcp_id_bin_map FROM 'data/id_bin_map.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS dcp_cpdb_agencyverified CASCADE;
CREATE TABLE dcp_cpdb_agencyverified (
    address character varying,
    agency character varying,
    bbl character varying,
    bin character varying,
    borough character varying,
    bridgeid character varying,
    category character varying,
    description character varying,
    mappable character varying,
    maprojid character varying,
    origin character varying,
    parkid character varying,
    parkname character varying,
    zipcode character varying
);
\COPY dcp_cpdb_agencyverified FROM 'data/dcp_cpdb_agencyverified.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE source_data_versions (
    schema_name character varying,
    v character varying
);