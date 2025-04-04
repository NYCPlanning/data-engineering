DROP TABLE IF EXISTS _doitt_buildingfootprints;
CREATE TABLE _doitt_buildingfootprints AS TABLE doitt_buildingfootprints;
ALTER TABLE _doitt_buildingfootprints RENAME COLUMN wkb_geometry TO geom;

DROP TABLE IF EXISTS _dpr_parksproperties;
CREATE TABLE _dpr_parksproperties AS TABLE dpr_parksproperties;
ALTER TABLE _dpr_parksproperties RENAME COLUMN wkb_geometry TO geom;

DROP TABLE IF EXISTS _dcp_facilities;
CREATE TABLE _dcp_facilities AS TABLE dcp_facilities;
ALTER TABLE _dcp_facilities RENAME COLUMN wkb_geometry TO geom;
