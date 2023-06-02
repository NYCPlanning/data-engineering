--Create DCP Attributes staging table
DROP TABLE IF EXISTS cpdb_dcpattributes CASCADE;
CREATE TABLE cpdb_dcpattributes AS( 
SELECT ccpversion, maprojid, magency, magencyacro, projectid, description FROM cpdb_projects
);
ALTER TABLE cpdb_dcpattributes
ADD typecategory text;
ALTER TABLE cpdb_dcpattributes
ADD geomsource text;
ALTER TABLE cpdb_dcpattributes
ADD dataname text;
ALTER TABLE cpdb_dcpattributes
ADD datasource text;
ALTER TABLE cpdb_dcpattributes
ADD datadate timestamp;

SELECT AddGeometryColumn ('public','cpdb_dcpattributes','geom',4326,'Geometry',2);
--------------------------------------------------------
