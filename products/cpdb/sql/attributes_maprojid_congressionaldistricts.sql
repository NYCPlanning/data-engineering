-- create maprojid --> congressional districts relational table
DROP TABLE IF EXISTS attributes_maprojid_congressionaldistricts;
-- spatial join
CREATE TABLE attributes_maprojid_congressionaldistricts AS
SELECT
    a.maprojid AS feature_id,
    'congdist'::text AS admin_boundary_type,
    b.congdist::text AS admin_boundary_id
FROM cpdb_dcpattributes AS a,
    dcp_congressionaldistricts AS b
WHERE a.geom && b.wkb_geometry AND ST_INTERSECTS(a.geom, b.wkb_geometry);
