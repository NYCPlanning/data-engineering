-- calculate how much (total area and percentage) of each lot is covered by a limited height district
-- assign the limited height district to each tax lot
-- the order limited height districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR more than a specified area of the lot if covered by the district

DROP TABLE IF EXISTS limitedheightperorder;
CREATE TABLE limitedheightperorder AS (
WITH 
limitedheightper AS (
SELECT p.bbl, n.lhlbl, 
  (ST_Area(CASE 
    WHEN ST_CoveredBy(p.geom, n.geom) 
    THEN p.geom 
    ELSE 
    ST_Multi(
      ST_Intersection(p.geom,n.geom)
      ) 
    END)) as segbblgeom,
  ST_Area(p.geom) as allbblgeom,
  (ST_Area(CASE 
    WHEN ST_CoveredBy(n.geom, p.geom) 
    THEN n.geom 
    ELSE 
    ST_Multi(
      ST_Intersection(n.geom,p.geom)
      ) 
    END)) as segzonegeom,
  ST_Area(n.geom) as allzonegeom
 FROM pluto AS p 
   INNER JOIN dcp_limitedheight AS n 
    ON ST_Intersects(p.geom, n.geom)
)
SELECT bbl, lhlbl, segbblgeom, (segbblgeom/allbblgeom)*100 as perbblgeom, (segzonegeom/allzonegeom)*100 as perzonegeom, ROW_NUMBER()
    	OVER (PARTITION BY bbl
      	ORDER BY segbblgeom DESC) AS row_number
  		FROM limitedheightper
);

UPDATE pluto a
SET ltdheight = lhlbl
FROM limitedheightperorder b
WHERE a.bbl=b.bbl 
AND perbblgeom >= 10;

DROP TABLE IF EXISTS limitedheightperorder;