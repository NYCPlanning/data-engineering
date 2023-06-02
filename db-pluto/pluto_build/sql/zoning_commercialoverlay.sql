-- calculate how much (total area and percentage) of each lot is covered by a commercial overlay
-- assign the commercial overlay(s) to each tax lot
-- the order commercial overlays are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR more than a specified area of the lot if covered by the district

DROP TABLE IF EXISTS commoverlayperorder;
CREATE TABLE commoverlayperorder AS (
WITH 
commoverlayper AS (
SELECT p.bbl, n.overlay, 
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
   INNER JOIN dcp_commercialoverlay AS n 
    ON ST_Intersects(p.geom, n.geom))
SELECT bbl, overlay, segbblgeom, (segbblgeom/allbblgeom)*100 as perbblgeom, (segzonegeom/allzonegeom)*100 as perzonegeom, ROW_NUMBER()
    	OVER (PARTITION BY bbl
      	ORDER BY segbblgeom DESC, segzonegeom DESC) AS row_number
  		FROM commoverlayper
);

UPDATE pluto a
SET overlay1 = overlay
FROM commoverlayperorder b
WHERE a.bbl=b.bbl 
AND row_number = 1
AND (perbblgeom >= 10
  OR perzonegeom >= 50);

UPDATE pluto a
SET overlay2 = overlay
FROM commoverlayperorder b
WHERE a.bbl=b.bbl 
AND row_number = 2
AND (perbblgeom >= 10
  OR perzonegeom >= 50);

DROP TABLE commoverlayperorder;