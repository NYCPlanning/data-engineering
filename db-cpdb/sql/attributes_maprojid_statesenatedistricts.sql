-- create maprojid --> state senate districts relational table
DROP TABLE IF EXISTS attributes_maprojid_statesenatedistricts;
-- spatial join
CREATE TABLE attributes_maprojid_statesenatedistricts AS (
  SELECT a.*
  FROM (
    SELECT a.maprojid AS feature_id,
       'statesenate'::text AS admin_boundary_type,
       b.stsendist::text AS admin_boundary_id
    FROM cpdb_dcpattributes a,
       dcp_statesenatedistricts b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
  ) a
);