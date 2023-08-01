-- create maprojid --> school districts relational table
DROP TABLE IF EXISTS attributes_maprojid_schooldistricts;
-- spatial join
CREATE TABLE attributes_maprojid_schooldistricts AS (
  SELECT a.*
  FROM (
    SELECT a.maprojid AS feature_id,
       'schooldistrict'::text AS admin_boundary_type,
       b.schooldist::text AS admin_boundary_id
    FROM cpdb_dcpattributes a,
       dcp_school_districts b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
  ) a
);