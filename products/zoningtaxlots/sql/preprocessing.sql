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
    WITH coalesced AS (
        SELECT
            bbl,
            coalesce(boro::text, left(bbl::text, 1)) AS boro,
            coalesce(block::text, substring(bbl::text, 2, 5)) AS block,
            coalesce(lot::text, substring(bbl::text, 7, 4)) AS lot,
            geom
        FROM dof_dtm
    )

    SELECT
        row_number() OVER () AS id,
        bbl,
        boro,
        block,
        lot,
        st_multi(st_union(st_makevalid(geom))) AS geom
    FROM coalesced
    GROUP BY bbl, boro, block, lot
);

DROP TABLE IF EXISTS dof_dtm;
ALTER TABLE dof_dtm_tmp
RENAME TO dof_dtm;

CREATE INDEX dof_dtm_wkb_geometry_geom_idx ON dof_dtm USING gist (geom gist_geometry_ops_2d);
