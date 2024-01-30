-- Create standardization of tables for aggregate scripts 

ALTER TABLE dcp_cdboundaries_wi RENAME COLUMN wkb_geometry TO geometry;
ALTER TABLE dcp_ct2020_wi RENAME COLUMN wkb_geometry TO geometry;
ALTER TABLE dcp_nta2020 RENAME COLUMN wkb_geometry TO geometry;
ALTER TABLE dcp_cdta2020 RENAME COLUMN wkb_geometry TO geometry;
