-- Preprocessing (change column names and table names) 
ALTER TABLE dcp_commercialoverlay 
RENAME COLUMN wkb_geometry TO geom;

ALTER TABLE dcp_limitedheight 
RENAME COLUMN wkb_geometry TO geom;

ALTER TABLE dcp_specialpurposesubdistricts 
RENAME COLUMN wkb_geometry TO geom;

ALTER TABLE dcp_specialpurpose
RENAME COLUMN wkb_geometry TO geom;

ALTER TABLE dcp_zoningdistricts 
RENAME COLUMN wkb_geometry TO geom;

ALTER TABLE dcp_zoningmapamendments 
RENAME COLUMN wkb_geometry TO geom;

ALTER TABLE dof_dtm 
RENAME COLUMN wkb_geometry TO geom;

ALTER TABLE dcp_zoningmapindex 
RENAME COLUMN wkb_geometry TO geom;

DROP TABLE IF EXISTS dof_dtm_tmp;
CREATE TABLE dof_dtm_tmp as(
    SELECT 
        bbl, 
        COALESCE(boro::text,LEFT(bbl::text, 1)) as boro, 
        COALESCE(block::text, SUBSTRING(bbl::text, 2, 5)) as block,
        COALESCE(lot::text, SUBSTRING(bbl::text, 7, 4)) as lot,
        ST_Multi(ST_Union(ST_MakeValid(f.geom))) as geom
    FROM dof_dtm As f
GROUP BY bbl, boro, block, lot);

DROP TABLE IF EXISTS dof_dtm;
ALTER TABLE dof_dtm_tmp
RENAME to dof_dtm;

CREATE INDEX dof_dtm_wkb_geometry_geom_idx ON dof_dtm USING GIST (geom gist_geometry_ops_2d);
