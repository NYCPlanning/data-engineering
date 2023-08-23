-- create maprojid --> state assembly districts relational table
DROP TABLE IF EXISTS attributes_maprojid_stateassemblydistricts;
-- spatial join
CREATE TABLE attributes_maprojid_stateassemblydistricts AS (
  SELECT a.*
  FROM (
    SELECT a.maprojid AS feature_id,
       'stateassembly'::text AS admin_boundary_type,
       b.assemdist::text AS admin_boundary_id
    FROM cpdb_dcpattributes a,
       dcp_stateassemblydistricts b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
    AND ST_GeometryType(b.wkb_geometry) = 'ST_MultiPolygon'
  ) a
);