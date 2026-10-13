-- data types follow the GIS template schema: https://github.com/NYCPlanning/gis-facilities-and-pops/blob/main/templates/_template_facilities_schema.json#L12
-- mapping of data types between postgres and geodabase reference: https://pro.arcgis.com/en/pro-app/3.1/help/data/geodatabases/manage-postgresql/data-types-postgresql.htm

-- create facdb table with expected column names and data types
DROP TABLE IF EXISTS facdb_export;
CREATE TABLE facdb_export AS
SELECT
    asset_id::INTEGER AS asset_id,
    rectype::VARCHAR(12) AS record_type,
    facname::VARCHAR(250) AS facility_name,
    addressnum::VARCHAR(12) AS address_number,
    streetname::VARCHAR(50) AS street_name,
    address::VARCHAR(150) AS address,
    city::VARCHAR(50) AS city,
    zipcode::VARCHAR(5) AS zip_code,
    factype::VARCHAR(250) AS facility_type,
    facsubgrp::VARCHAR(100) AS facility_subgroup,
    facgroup::VARCHAR(100) AS facility_group,
    facdomain::VARCHAR(100) AS facility_domain,
    servarea::VARCHAR(10) AS service_area,
    opname::VARCHAR(150) AS operating_entity_name,
    opabbrev::VARCHAR(12) AS operating_entity_abbreviation,
    optype::VARCHAR(12) AS operator_type,
    overagency::VARCHAR(150) AS oversight_agency_name,
    overabbrev::VARCHAR(12) AS oversight_agency_abbreviation,
    overlevel::VARCHAR(12) AS oversight_agency_level,
    capacity::INT AS capacity,
    captype::VARCHAR(12) AS capacity_unit_type,
    fcs_score_total::SMALLINT AS fcs_score_total,
    fcs_letter_grade_total::VARCHAR(1) AS fcs_letter_grade_total,
    fcs_score_architectural::SMALLINT AS fcs_score_architectural,
    fcs_letter_grade_architectural::VARCHAR(1) AS fcs_letter_grade_architectural,
    fcs_score_systems::SMALLINT AS fcs_score_systems,
    fcs_letter_grade_systems::VARCHAR(1) AS fcs_letter_grade_systems,
    fcs_assessment_year::SMALLINT AS fcs_assessment_year,
    fcs_assessment_month::SMALLINT AS fcs_assessment_month,
    fcs_assessment_date::DATE AS fcs_assessment_date,
    boro::VARCHAR(15) AS borough,
    borocode::SMALLINT AS borough_code,
    bin::INT AS bin,
    bbl::NUMERIC(10) AS bbl,
    latitude::FLOAT AS latitude,
    longitude::FLOAT AS longitude,
    xcoord::FLOAT AS x_coordinate,
    ycoord::FLOAT AS y_coordinate,
    cd::SMALLINT AS community_district,
    nta2010::VARCHAR(6) AS nta_2010,
    nta2020::VARCHAR(6) AS nta_2020,
    council::SMALLINT AS city_council_district,
    ct2010::VARCHAR(6) AS census_tract_2010,
    ct2020::VARCHAR(6) AS census_tract_2020,
    schooldist::VARCHAR(3) AS school_district,
    policeprct::SMALLINT AS police_precinct,
    datasource::VARCHAR(150) AS data_source_file,
    uid::VARCHAR AS uid,
    geom
FROM facdb;

-- replace empty strings ('' or ' ') with NULL before deriving the other exports
CALL replace_empty_strings(:'build_schema', 'facdb_export');

-- create facdb table without geometry column
DROP TABLE IF EXISTS facdb_export_csv;
CREATE TABLE facdb_export_csv AS
SELECT * FROM facdb_export;
ALTER TABLE facdb_export_csv DROP COLUMN geom;

-- shapefile (DBF) field names max out at 10 characters, so the shapefile gets
-- the short source-style names instead of the descriptive ones
DROP TABLE IF EXISTS facdb_export_shp;
CREATE TABLE facdb_export_shp AS
SELECT
    asset_id AS "ASSET_ID",
    record_type AS "RECTYPE",
    facility_name AS "FACNAME",
    address_number AS "ADDRESSNUM",
    street_name AS "STREETNAME",
    address AS "ADDRESS",
    city AS "CITY",
    zip_code AS "ZIPCODE",
    facility_type AS "FACTYPE",
    facility_subgroup AS "FACSUBGRP",
    facility_group AS "FACGROUP",
    facility_domain AS "FACDOMAIN",
    service_area AS "SERVAREA",
    operating_entity_name AS "OPNAME",
    operating_entity_abbreviation AS "OPABBREV",
    operator_type AS "OPTYPE",
    oversight_agency_name AS "OVERAGENCY",
    oversight_agency_abbreviation AS "OVERABBREV",
    oversight_agency_level AS "OVERLEVEL",
    capacity AS "CAPACITY",
    capacity_unit_type AS "CAPTYPE",
    fcs_score_total AS "FCS_NUM",
    fcs_letter_grade_total AS "FCS_LTR",
    fcs_score_architectural AS "FCS_NUMARC",
    fcs_letter_grade_architectural AS "FCS_LTRARC",
    fcs_score_systems AS "FCS_NUMSYS",
    fcs_letter_grade_systems AS "FCS_LTRSYS",
    fcs_assessment_year AS "FCS_ASMTYR",
    fcs_assessment_month AS "FCS_ASMTMO",
    fcs_assessment_date AS "FCS_ASMTDT",
    borough AS "BORO",
    borough_code AS "BOROCODE",
    bin AS "BIN",
    bbl AS "BBL",
    latitude AS "LATITUDE",
    longitude AS "LONGITUDE",
    x_coordinate AS "XCOORD",
    y_coordinate AS "YCOORD",
    community_district AS "CD",
    nta_2010 AS "NTA2010",
    nta_2020 AS "NTA2020",
    city_council_district AS "COUNCIL",
    census_tract_2010 AS "CT2010",
    census_tract_2020 AS "CT2020",
    school_district AS "SCHOOLDIST",
    police_precinct AS "POLICEPRCT",
    data_source_file AS "DATASOURCE",
    uid AS "UID",
    geom
FROM facdb_export;
