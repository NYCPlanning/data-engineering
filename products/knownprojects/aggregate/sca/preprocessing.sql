-- Create standardization of tables for sca aggregate scripts

ALTER TABLE doe_eszones RENAME COLUMN wkb_geometry TO geometry;
ALTER TABLE doe_school_subdistricts RENAME COLUMN wkb_geometry TO geometry;
ALTER TABLE dcp_school_districts RENAME COLUMN wkb_geometry TO geometry;
