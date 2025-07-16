-- create maprojid --> council districts relational table
DROP TABLE IF EXISTS attributes_maprojid_councildistricts;
-- spatial join
CREATE TABLE attributes_maprojid_councildistricts AS (
    SELECT a.*
    FROM (
        SELECT
            a.maprojid AS feature_id,
            'council'::text AS admin_boundary_type,
            b.coundist::text AS admin_boundary_id
        FROM cpdb_dcpattributes AS a,
            dcp_councildistricts AS b
        WHERE st_intersects(a.geom, b.wkb_geometry)
    ) AS a
);
