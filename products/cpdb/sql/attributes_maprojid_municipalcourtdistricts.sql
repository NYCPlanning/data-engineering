-- create maprojid --> municipal courtdistricts relational table
DROP TABLE IF EXISTS attributes_maprojid_municipalcourtdistricts;
-- spatial join
CREATE TABLE attributes_maprojid_municipalcourtdistricts AS (
  SELECT a.*
  FROM (
    SELECT a.maprojid AS feature_id,
       'municourt'::text AS admin_boundary_type,
       b.municourt::text AS admin_boundary_id
    FROM cpdb_dcpattributes a,
       dcp_municipalcourtdistricts b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
  ) a
);