DROP TABLE IF EXISTS validdtm; 
CREATE TABLE validdtm AS (
  SELECT 
    a.bbl, 
    ST_MakeValid(a.geom) as geom 
  FROM dof_dtm a
  WHERE ST_GeometryType(ST_MakeValid(a.geom)) = 'ST_MultiPolygon'
);
CREATE INDEX validdtm_geom_idx ON validdtm USING GIST (geom gist_geometry_ops_2d);

ANALYZE validdtm;

DROP TABLE IF EXISTS validzones; 
CREATE TABLE validzones AS (
  SELECT 
    a.zonedist, 
    ST_MakeValid(a.geom) as geom  
  FROM dcp_zoningdistricts a
  WHERE ST_GeometryType(ST_MakeValid(a.geom)) = 'ST_MultiPolygon'
); 
CREATE INDEX validzones_geom_idx ON validzones USING GIST (geom gist_geometry_ops_2d);

ANALYZE validzones;

DROP TABLE IF EXISTS lotzoneper; 
CREATE TABLE lotzoneper AS (
  SELECT 
    p.bbl, 
    n.zonedist, 
    (
      ST_Area(
        CASE 
          WHEN ST_CoveredBy(p.geom, n.geom) THEN p.geom::geography
          ELSE ST_Multi(ST_Intersection(p.geom, n.geom))::geography
        END)
    ) AS segbblgeom,

    (
      ST_Area(
        CASE 
          WHEN ST_CoveredBy(n.geom, p.geom) THEN n.geom::geography 
          ELSE ST_Multi(ST_Intersection(n.geom, p.geom))::geography
        END
      )
    ) AS segzonegeom,
    ST_Area(p.geom::geography) AS allbblgeom,
    ST_Area(n.geom::geography) AS allzonegeom
  FROM validdtm AS p INNER JOIN validzones AS n 
  ON ST_Intersects(p.geom, n.geom)
);

ANALYZE lotzoneper;

DROP TABLE IF EXISTS lotzoneperorder; 
CREATE TABLE lotzoneperorder AS (
  SELECT 
    bbl, 
    zonedist, 
    segbblgeom, 
    allbblgeom, 
    (segbblgeom/allbblgeom)*100 as perbblgeom, 
    segzonegeom, 
    allzonegeom, 
    (segzonegeom/allzonegeom)*100 as perzonegeom, 
    ROW_NUMBER() OVER (
        PARTITION BY bbl
        ORDER BY segbblgeom DESC
    ) AS row_number
  FROM lotzoneper
);

-- calculate how much (total area and percentage) of each lot is covered by a zoning district
-- assign the zoning district(s) to each tax lot
-- the order zoning districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR the majority of the district is within the lot
-- split and process by borough to improve processing time
-- join each of the boro tables into one table
-- update each of zoning district fields
-- only say that a lot is within a zoning district if
-- more than 10% of the lot is coverd by the zoning district
-- or more than a specified area is covered by the district
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

UPDATE dcp_zoning_taxlot a
SET zoningdistrict1 = zonedist
FROM lotzoneperorder b
WHERE a.bbl=b.bbl 
AND row_number = 1
AND ROUND(perbblgeom::numeric,2) >= 10;

-- if the largest zoning district is under 10% of entire lot 
-- (e.g. water front lots) 
-- then assign the largest zoning district to be zonedist1
UPDATE dcp_zoning_taxlot a
SET zoningdistrict1 = zonedist
FROM lotzoneperorder b
WHERE a.bbl=b.bbl 
  AND a.zoningdistrict1 is null
  AND row_number = 1;

UPDATE dcp_zoning_taxlot a
SET zoningdistrict2 = zonedist
FROM lotzoneperorder b
WHERE a.bbl=b.bbl 
AND row_number = 2
AND  ROUND(perbblgeom::numeric,2) >= 10;

UPDATE dcp_zoning_taxlot a
SET zoningdistrict3 = zonedist
FROM lotzoneperorder b
WHERE a.bbl=b.bbl 
AND row_number = 3 
AND  ROUND(perbblgeom::numeric,2) >= 10;

UPDATE dcp_zoning_taxlot a
SET zoningdistrict4 = zonedist
FROM lotzoneperorder b
WHERE a.bbl=b.bbl 
AND row_number = 4
AND  ROUND(perbblgeom::numeric,2) >= 10;
