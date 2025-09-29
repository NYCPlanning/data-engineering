-- Create standardization of tables for sca aggregate scripts

ALTER TABLE doe_eszones RENAME COLUMN wkb_geometry TO geometry;
ALTER TABLE doe_school_subdistricts RENAME COLUMN geom TO geometry;
ALTER TABLE dcp_school_districts RENAME COLUMN wkb_geometry TO geometry;

-- Create standardization of tables for other aggregate scripts 

ALTER TABLE dcp_cdboundaries_wi RENAME COLUMN wkb_geometry TO geometry;
ALTER TABLE dcp_ct2020_wi RENAME COLUMN wkb_geometry TO geometry;
ALTER TABLE dcp_nta2020 RENAME COLUMN wkb_geometry TO geometry;
ALTER TABLE dcp_cdta2020 RENAME COLUMN wkb_geometry TO geometry;
