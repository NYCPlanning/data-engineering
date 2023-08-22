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
CREATE TABLE dof_dtm_tmp AS (
    SELECT
        bbl,
        COALESCE(boro::text, LEFT(bbl::text, 1)) AS boro,
        COALESCE(block::text, SUBSTRING(bbl::text, 2, 5)) AS block,
        COALESCE(lot::text, SUBSTRING(bbl::text, 7, 4)) AS lot,
        ST_MULTI(ST_UNION(ST_MAKEVALID(f.geom))) AS geom
    FROM dof_dtm AS f
    GROUP BY bbl, boro, block, lot
);

DROP TABLE IF EXISTS dof_dtm;
ALTER TABLE dof_dtm_tmp
RENAME TO dof_dtm;

CREATE INDEX dof_dtm_wkb_geometry_geom_idx ON dof_dtm USING gist (geom gist_geometry_ops_2d);
