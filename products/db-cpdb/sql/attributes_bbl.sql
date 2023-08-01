-- spatial join to get bbls

WITH bx as (
  SELECT bbl,
         ST_Transform(ST_SetSRID(geom,2263),4326) as geom
  FROM bxmappluto
  )
UPDATE cpdb_dcpattributes a
SET bbl = b.bbl
FROM bx b
WHERE a.bbl IS NULL AND
      ST_Within(a.geom, b.geom);

WITH bk as (
  SELECT bbl,
         ST_Transform(ST_SetSRID(geom,2263),4326) as geom
  FROM bkmappluto
  )
UPDATE cpdb_dcpattributes a
SET bbl = b.bbl
FROM bk b
WHERE a.bbl IS NULL AND
      ST_Within(a.geom, b.geom);

WITH mn as (
  SELECT bbl,
         ST_Transform(ST_SetSRID(geom,2263),4326) as geom
  FROM mnmappluto
  )
UPDATE cpdb_dcpattributes a
SET bbl = b.bbl
FROM mn b
WHERE a.bbl IS NULL AND
      ST_Within(a.geom, b.geom);

WITH qn as (
  SELECT bbl,
         ST_Transform(ST_SetSRID(geom,2263),4326) as geom
  FROM qnmappluto
  )
UPDATE cpdb_dcpattributes a
SET bbl = b.bbl
FROM qn b
WHERE a.bbl IS NULL AND
      ST_Within(a.geom, b.geom);

WITH si as (
  SELECT bbl,
         ST_Transform(ST_SetSRID(geom,2263),4326) as geom
  FROM simappluto
  )
UPDATE cpdb_dcpattributes a
SET bbl = b.bbl
FROM si b
WHERE a.bbl IS NULL AND
      ST_Within(a.geom, b.geom);
