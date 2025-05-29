CREATE EXTENSION IF NOT EXISTS tablefunc;

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
    cape_agencycode char(3),
    cape_agencyacronym text,
    cape_agency text,
    agency_class text
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

DROP TABLE IF EXISTS type_category_patterns CASCADE;
CREATE TABLE type_category_patterns (
    pattern character varying,
    typecategory character varying
);

CREATE TEMP TABLE staging_patterns (
    pattern character varying
);

\COPY staging_patterns FROM 'data/type_patterns_ittvequ.csv' DELIMITER ',' CSV;
INSERT INTO type_category_patterns (pattern, typecategory)
SELECT
    pattern,
    'ITT, Vehicles, and Equipment' AS typecategory
FROM staging_patterns;
DELETE FROM staging_patterns;

\COPY staging_patterns FROM 'data/type_patterns_lump_sum.csv' DELIMITER ',' CSV;
INSERT INTO type_category_patterns (pattern, typecategory)
SELECT
    pattern,
    'Lump Sum' AS typecategory
FROM staging_patterns;
DELETE FROM staging_patterns;

\COPY staging_patterns FROM 'data/type_patterns_fixed_asset.csv' DELIMITER ',' CSV;
INSERT INTO type_category_patterns (pattern, typecategory)
SELECT
    pattern,
    'Fixed Asset' AS typecategory
FROM staging_patterns;
