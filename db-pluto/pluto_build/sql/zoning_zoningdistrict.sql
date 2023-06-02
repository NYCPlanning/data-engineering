-- calculate how much (total area and percentage) of each lot is covered by a zoning district
-- assign the zoning district(s) to each tax lot
-- the order zoning districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR the majority of the district is within the lot

WITH new_order AS(
  SELECT bbl, zonedist, ROW_NUMBER()
  OVER(PARTITION BY bbl ORDER BY priority ASC) AS row_number
    FROM (
      SELECT * FROM lotzoneperorder
      WHERE bbl in(SELECT bbl from(
        SELECT bbl, MAX(segbblgeom) - MIN(segbblgeom) as diff 
        FROM lotzoneperorder
        WHERE perbblgeom >= 10
        group by bbl
      ) a WHERE diff > 0 and diff < 0.01))a 
      JOIN zonedist_priority 
      USING (zonedist))
UPDATE lotzoneperorder
SET row_number = new_order.row_number
FROM new_order
WHERE lotzoneperorder.bbl = new_order.bbl 
  AND lotzoneperorder.zonedist = new_order.zonedist; 

-- update each of zoning district fields
-- only say that a lot is within a zoning district if
-- more than 10% of the lot is coverd by the zoning district
-- or more than a specified area is covered by the district
UPDATE pluto a
SET zonedist1 = zonedist
FROM lotzoneperorder b
WHERE a.bbl=b.bbl 
AND row_number = 1
AND perbblgeom >= 10;

-- if the largest zoning district is under 10% of entire lot 
-- (e.g. water front lots) 
-- then assign the largest zoning district to be zonedist1
UPDATE pluto a
SET zonedist1 = zonedist
FROM lotzoneperorder b
WHERE a.bbl=b.bbl 
  AND a.zonedist1 is null
  AND row_number = 1;

UPDATE pluto a
SET zonedist2 = zonedist
FROM lotzoneperorder b
WHERE a.bbl=b.bbl 
AND row_number = 2
AND perbblgeom >= 10;

UPDATE pluto a
SET zonedist3 = zonedist
FROM lotzoneperorder b
WHERE a.bbl=b.bbl 
AND row_number = 3
AND perbblgeom >= 10;

UPDATE pluto a
SET zonedist4 = zonedist
FROM lotzoneperorder b
WHERE a.bbl=b.bbl 
AND row_number = 4
AND perbblgeom >= 10;

-- drop the area table
-- DROP TABLE lotzoneperorder;

-- for lots without a zoningdistrict1
-- assign the zoning district that is 
-- within 25 feet or 7 meters
-- DROP TABLE IF EXISTS lotzonedistance;
-- CREATE TABLE lotzonedistance AS (
-- WITH validdtm AS (
--   SELECT a.bbl, a.geom 
--   FROM dof_dtm a
--   WHERE ST_GeometryType(ST_MakeValid(a.geom)) = 'ST_MultiPolygon' 
--   AND a.bbl IN (SELECT bbl FROM dcp_zoning_taxlot WHERE zoningdistrict1 IS NULL)),
-- validzones AS (
--   SELECT a.zonedist, a.geom 
--   FROM dcp_zoningdistricts a
--   WHERE ST_GeometryType(ST_MakeValid(a.geom)) = 'ST_MultiPolygon')
-- SELECT a.bbl, b.zonedist
-- FROM validdtm a, validzones b
-- WHERE ST_DWithin(a.geom::geography, b.geom::geography, 7));

-- UPDATE dcp_zoning_taxlot a
-- SET zoningdistrict1 = zonedist
-- FROM lotzonedistance b
-- WHERE a.bbl=b.bbl 
-- AND zoningdistrict1 IS NULL;


