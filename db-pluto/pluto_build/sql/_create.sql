DROP TABLE IF EXISTS pluto_input_research;
CREATE TABLE pluto_input_research (
    bbl text,
    field text,
    old_value text,
    new_value text,
    type text,
    reason text,
    version text
);
\COPY pluto_input_research FROM 'data/pluto_input_research.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS pluto_input_landuse_bldgclass;
CREATE TABLE pluto_input_landuse_bldgclass (
    bldgclass text,
    landuse text,
    landusevalue text
);
\COPY pluto_input_landuse_bldgclass FROM 'data/pluto_input_landuse_bldgclass.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS pluto_input_condolot_descriptiveattributes;
CREATE TABLE pluto_input_condolot_descriptiveattributes (
    CondNO text,
    Boro text,
    PARID text,
    BC text,
    TC text,
    LandSize text,
    Story text,
    YearBuilt text
);
\COPY pluto_input_condolot_descriptiveattributes FROM 'data/pluto_input_condolot_descriptiveattributes.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS pluto_input_condo_bldgclass;
CREATE TABLE pluto_input_condo_bldgclass (
    code character varying,
    description character varying,
    type character varying,
    dcpcreated character varying,
    logic character varying
);
\COPY pluto_input_condo_bldgclass FROM 'data/pluto_input_condo_bldgclass.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS pluto_input_bsmtcode;
CREATE TABLE pluto_input_bsmtcode (
    bsmnt_type character varying,
    bsmntgradient character varying,
    bsmtcode character varying,
    bsmnt_typevalue character varying,
    bsmntgradientvalue character varying,
    bsmtcodevalue character varying
);
\COPY pluto_input_bsmtcode FROM 'data/pluto_input_bsmtcode.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS dcp_zoning_maxfar;
CREATE TABLE dcp_zoning_maxfar (
    zonedist character varying,
    contextual character varying,
    zoningdistricttype character varying,
    residfar character varying,
    facilfar character varying,
    commfar character varying,
    mnffar character varying,
    column1 character varying,
    verified character varying,
    extra character varying
);
\COPY dcp_zoning_maxfar FROM 'data/dcp_zoning_maxfar.csv' DELIMITER ',' CSV HEADER;
