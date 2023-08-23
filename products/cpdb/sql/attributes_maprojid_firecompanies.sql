-- create maprojid --> fire companies relational table
DROP TABLE IF EXISTS attributes_maprojid_firecompanies;
-- spatial join
CREATE TABLE attributes_maprojid_firecompanies AS (
SELECT a.*
  FROM (
    SELECT a.maprojid AS feature_id,
        'fireconum'::text AS admin_boundary_type,
       b.fire_co_num::text AS admin_boundary_id
      FROM cpdb_dcpattributes a,
       fdny_firecompanies b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
  ) a
  UNION ALL
  SELECT b.*
  FROM (
    SELECT a.maprojid AS feature_id,
        'firecotype'::text AS admin_boundary_type,
       b.fire_co_type::text AS admin_boundary_id
      FROM cpdb_dcpattributes a,
       fdny_firecompanies b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
  ) b
  UNION ALL
  SELECT c.*
  FROM (
    SELECT a.maprojid AS feature_id,
        'firebn'::text AS admin_boundary_type,
       b.fire_bn::text AS admin_boundary_id
      FROM cpdb_dcpattributes a,
       fdny_firecompanies b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
  ) c
  UNION ALL
  SELECT d.*
  FROM (
    SELECT a.maprojid AS feature_id,
        'firediv'::text AS admin_boundary_type,
       b.fire_div::text AS admin_boundary_id
      FROM cpdb_dcpattributes a,
       fdny_firecompanies b
    WHERE ST_Intersects(a.geom, b.wkb_geometry)
  ) d
);