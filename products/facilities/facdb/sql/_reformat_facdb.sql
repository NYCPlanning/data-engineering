-- modify facdb column names and their data types based on GIS template schema: https://github.com/NYCPlanning/gis-facilities-and-pops/blob/main/templates/_template_facilities_schema.json#L12
-- mapping of data types between postgres and geodabase reference: https://pro.arcgis.com/en/pro-app/3.1/help/data/geodatabases/manage-postgresql/data-types-postgresql.htm

-- create facdb table with expected column names and data types
CREATE TABLE facdb_export AS
SELECT
    facname::VARCHAR(250) AS "FACNAME",
    addressnum::VARCHAR(12) AS "ADDRESSNUM",
    streetname::VARCHAR(50) AS "STREETNAME",
    address::VARCHAR(150) AS "ADDRESS",
    city::VARCHAR(50) AS "CITY",
    zipcode::VARCHAR(5) AS "ZIPCODE",
    factype::VARCHAR(250) AS "FACTYPE",
    facsubgrp::VARCHAR(100) AS "FACSUBGRP",
    facgroup::VARCHAR(100) AS "FACGROUP",
    facdomain::VARCHAR(100) AS "FACDOMAIN",
    servarea::VARCHAR(10) AS "SERVAREA",
    opname::VARCHAR(150) AS "OPNAME",
    opabbrev::VARCHAR(12) AS "OPABBREV",
    optype::VARCHAR(12) AS "OPTYPE",
    overagency::VARCHAR(150) AS "OVERAGENCY",
    overabbrev::VARCHAR(12) AS "OVERABBREV",
    overlevel::VARCHAR(12) AS "OVERLEVEL",
    capacity::INT AS "CAPACITY",
    captype::VARCHAR(12) AS "CAPTYPE",
    boro::VARCHAR(15) AS "BORO",
    bin::INT AS "BIN",
    bbl::NUMERIC(10) AS "BBL",
    latitude::FLOAT AS "LATITUDE",
    longitude::FLOAT AS "LONGITUDE",
    xcoord::FLOAT AS "XCOORD",
    ycoord::FLOAT AS "YCOORD",
    cd::SMALLINT AS "CD",
    nta2010::VARCHAR(6) AS "NTA2010",
    nta2020::VARCHAR(6) AS "NTA2020",
    council::SMALLINT AS "COUNCIL",
    ct2010::VARCHAR(6) AS "CT2010",
    ct2020::VARCHAR(6) AS "CT2020",
    borocode::SMALLINT AS "BOROCODE",
    schooldist::VARCHAR(3) AS "SCHOOLDIST",
    policeprct::SMALLINT AS "POLICEPRCT",
    datasource::VARCHAR(150) AS "DATASOURCE",
    uid::VARCHAR AS "UID",
    geom
FROM facdb;


-- create facdb table without geometry column
CREATE TABLE facdb_export_csv AS
SELECT
    facname::VARCHAR(250) AS "FACNAME",
    addressnum::VARCHAR(12) AS "ADDRESSNUM",
    streetname::VARCHAR(50) AS "STREETNAME",
    address::VARCHAR(150) AS "ADDRESS",
    city::VARCHAR(50) AS "CITY",
    zipcode::VARCHAR(5) AS "ZIPCODE",
    factype::VARCHAR(250) AS "FACTYPE",
    facsubgrp::VARCHAR(100) AS "FACSUBGRP",
    facgroup::VARCHAR(100) AS "FACGROUP",
    facdomain::VARCHAR(100) AS "FACDOMAIN",
    servarea::VARCHAR(10) AS "SERVAREA",
    opname::VARCHAR(150) AS "OPNAME",
    opabbrev::VARCHAR(12) AS "OPABBREV",
    optype::VARCHAR(12) AS "OPTYPE",
    overagency::VARCHAR(150) AS "OVERAGENCY",
    overabbrev::VARCHAR(12) AS "OVERABBREV",
    overlevel::VARCHAR(12) AS "OVERLEVEL",
    capacity::INT AS "CAPACITY",
    captype::VARCHAR(12) AS "CAPTYPE",
    boro::VARCHAR(15) AS "BORO",
    bin::INT AS "BIN",
    bbl::NUMERIC(10) AS "BBL",
    latitude::FLOAT AS "LATITUDE",
    longitude::FLOAT AS "LONGITUDE",
    xcoord::FLOAT AS "XCOORD",
    ycoord::FLOAT AS "YCOORD",
    cd::SMALLINT AS "CD",
    nta2010::VARCHAR(6) AS "NTA2010",
    nta2020::VARCHAR(6) AS "NTA2020",
    council::SMALLINT AS "COUNCIL",
    ct2010::VARCHAR(6) AS "CT2010",
    ct2020::VARCHAR(6) AS "CT2020",
    borocode::SMALLINT AS "BOROCODE",
    schooldist::VARCHAR(3) AS "SCHOOLDIST",
    policeprct::SMALLINT AS "POLICEPRCT",
    datasource::VARCHAR(150) AS "DATASOURCE",
    uid::VARCHAR AS "UID"
FROM facdb;

-- replace empty strings ('' or ' ') with NULL value in facdb
CALL replace_empty_strings(:'build_schema', 'facdb_export');
CALL replace_empty_strings(:'build_schema', 'facdb_export_csv')
