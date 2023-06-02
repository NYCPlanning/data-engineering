-- create maprojid --> congressional districts relational table
DROP TABLE IF EXISTS attributes_maprojid_congressionaldistricts;
-- spatial join
CREATE TABLE attributes_maprojid_congressionaldistricts AS (
  SELECT a.*
  FROM (
    SELECT a.maprojid AS feature_id,
    	'congdist'::text AS admin_boundary_type,
       b.congdist::text AS admin_boundary_id
    FROM cpdb_dcpattributes a,
       dcp_congressionaldistricts b
    WHERE a.geom&&b.wkb_geometry and ST_Intersects(a.geom, b.wkb_geometry)) a
);