-- create maprojid --> traffic analysis zones relational table
DROP TABLE IF EXISTS attributes_maprojid_trafficanalysiszones;
-- spatial join
CREATE TABLE attributes_maprojid_trafficanalysiszones AS (
  SELECT a.*
  FROM (
    SELECT a.maprojid AS feature_id,
    'taz'::text AS admin_boundary_type,
       b.geoid10::text AS admin_boundary_id
    FROM cpdb_dcpattributes a,
       dcp_trafficanalysiszones b
    WHERE ST_Intersects(a.geom, ST_SetSRID(b.wkb_geometry, 4326))
  ) a
);
