-- create maprojid --> census tracts, puma, nta, and boro relational table
DROP TABLE IF EXISTS attributes_maprojid_censustracts;
-- spatial join
CREATE TABLE attributes_maprojid_censustracts AS (
  SELECT a.*
  FROM (
    SELECT a.maprojid AS feature_id,
        'borocode'::text AS admin_boundary_type,
       b.borocode::text AS admin_boundary_id
      FROM cpdb_dcpattributes a,
       dcp_ct2020 b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
  ) a
  UNION ALL
  SELECT b.*
  FROM (
    SELECT a.maprojid AS feature_id,
        'nta'::text AS admin_boundary_type,
       b.nta2020::text AS admin_boundary_id
      FROM cpdb_dcpattributes a,
       dcp_ct2020 b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
  ) b
  UNION ALL
  SELECT d.*
  FROM (
    SELECT a.maprojid AS feature_id,
        'censtract'::text AS admin_boundary_type,
       b.boroct2020::text AS admin_boundary_id
      FROM cpdb_dcpattributes a,
       dcp_ct2020 b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
  ) d
)
;
