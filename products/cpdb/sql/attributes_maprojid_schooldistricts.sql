-- create maprojid --> school districts relational table
DROP TABLE IF EXISTS attributes_maprojid_schooldistricts;
-- spatial join
CREATE TABLE attributes_maprojid_schooldistricts AS
SELECT
    a.maprojid AS feature_id,
    'schooldistrict'::text AS admin_boundary_type,
    b.schooldist::text AS admin_boundary_id
FROM cpdb_dcpattributes AS a,
    dcp_school_districts AS b
WHERE ST_INTERSECTS(a.geom, b.wkb_geometry);
