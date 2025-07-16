-- modify facdb column names and their data types based on GIS template schema: https://github.com/NYCPlanning/gis-facilities-and-pops/blob/main/templates/_template_facilities_schema.json#L12
-- mapping of data types between postgres and geodabase reference: https://pro.arcgis.com/en/pro-app/3.1/help/data/geodatabases/manage-postgresql/data-types-postgresql.htm

-- create facdb table with expected column names and data types
CREATE TABLE temp_facdb AS
SELECT
    facname::varchar(250) AS "FACNAME",
    addressnum::varchar(12) AS "ADDRESSNUM",
    streetname::varchar(50) AS "STREETNAME",
    address::varchar(150) AS "ADDRESS",
    city::varchar(50) AS "CITY",
    zipcode::varchar(5) AS "ZIPCODE",
    factype::varchar(250) AS "FACTYPE",
    facsubgrp::varchar(100) AS "FACSUBGRP",
    facgroup::varchar(100) AS "FACGROUP",
    facdomain::varchar(100) AS "FACDOMAIN",
    servarea::varchar(10) AS "SERVAREA",
    opname::varchar(150) AS "OPNAME",
    opabbrev::varchar(12) AS "OPABBREV",
    optype::varchar(12) AS "OPTYPE",
    overagency::varchar(150) AS "OVERAGENCY",
    overabbrev::varchar(12) AS "OVERABBREV",
    overlevel::varchar(12) AS "OVERLEVEL",
    capacity::int AS "CAPACITY",
    captype::varchar(12) AS "CAPTYPE",
    boro::varchar(15) AS "BORO",
    bin::int AS "BIN",
    bbl::numeric(10) AS "BBL",
    latitude::float AS "LATITUDE",
    longitude::float AS "LONGITUDE",
    xcoord::float AS "XCOORD",
    ycoord::float AS "YCOORD",
    cd::smallint AS "CD",
    nta2010::varchar(6) AS "NTA2010",
    nta2020::varchar(6) AS "NTA2020",
    council::smallint AS "COUNCIL",
    ct2010::varchar(6) AS "CT2010",
    ct2020::varchar(6) AS "CT2020",
    borocode::smallint AS "BOROCODE",
    schooldist::varchar(3) AS "SCHOOLDIST",
    policeprct::smallint AS "POLICEPRCT",
    datasource::varchar(150) AS "DATASOURCE",
    uid::varchar AS "UID",
    geom
FROM facdb;


-- create facdb table without geometry column
CREATE TABLE facdb_without_geom_col AS
SELECT
    facname::varchar(250) AS "FACNAME",
    addressnum::varchar(12) AS "ADDRESSNUM",
    streetname::varchar(50) AS "STREETNAME",
    address::varchar(150) AS "ADDRESS",
    city::varchar(50) AS "CITY",
    zipcode::varchar(5) AS "ZIPCODE",
    factype::varchar(250) AS "FACTYPE",
    facsubgrp::varchar(100) AS "FACSUBGRP",
    facgroup::varchar(100) AS "FACGROUP",
    facdomain::varchar(100) AS "FACDOMAIN",
    servarea::varchar(10) AS "SERVAREA",
    opname::varchar(150) AS "OPNAME",
    opabbrev::varchar(12) AS "OPABBREV",
    optype::varchar(12) AS "OPTYPE",
    overagency::varchar(150) AS "OVERAGENCY",
    overabbrev::varchar(12) AS "OVERABBREV",
    overlevel::varchar(12) AS "OVERLEVEL",
    capacity::int AS "CAPACITY",
    captype::varchar(12) AS "CAPTYPE",
    boro::varchar(15) AS "BORO",
    bin::int AS "BIN",
    bbl::numeric(10) AS "BBL",
    latitude::float AS "LATITUDE",
    longitude::float AS "LONGITUDE",
    xcoord::float AS "XCOORD",
    ycoord::float AS "YCOORD",
    cd::smallint AS "CD",
    nta2010::varchar(6) AS "NTA2010",
    nta2020::varchar(6) AS "NTA2020",
    council::smallint AS "COUNCIL",
    ct2010::varchar(6) AS "CT2010",
    ct2020::varchar(6) AS "CT2020",
    borocode::smallint AS "BOROCODE",
    schooldist::varchar(3) AS "SCHOOLDIST",
    policeprct::smallint AS "POLICEPRCT",
    datasource::varchar(150) AS "DATASOURCE",
    uid::varchar AS "UID"
FROM facdb;

-- delete old facdb table and rename the new one
DROP TABLE facdb;
ALTER TABLE temp_facdb RENAME TO facdb;

-- replace empty strings ('' or ' ') with NULL value in facdb
CALL replace_empty_strings(:'build_schema', 'facdb');
CALL replace_empty_strings(:'build_schema', 'facdb_without_geom_col')
