-- calculate how much (total area and percentage) of each lot is covered by a special purpose district
-- assign the special purpose district(s) to each tax lot
-- the order special purpose districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR more than a specified area of the lot if covered by the district
DROP TABLE IF EXISTS specialpurposeperorder;
CREATE TABLE specialpurposeperorder AS (
WITH 
specialpurposeper AS (
SELECT p.bbl, n.sdlbl,
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
   INNER JOIN dcp_specialpurpose AS n 
    ON ST_Intersects(p.geom, n.geom)
)
SELECT bbl, sdlbl, segbblgeom, (segbblgeom/allbblgeom)*100 as perbblgeom, (segzonegeom/allzonegeom)*100 as perzonegeom, ROW_NUMBER()
    	OVER (PARTITION BY bbl
      	ORDER BY segbblgeom DESC) AS row_number
  		FROM specialpurposeper
);

UPDATE pluto a
SET spdist1 = sdlbl
FROM specialpurposeperorder b
WHERE a.bbl=b.bbl 
AND row_number = 1
AND perbblgeom >= 10;

UPDATE pluto a
SET spdist2 = sdlbl
FROM specialpurposeperorder b
WHERE a.bbl=b.bbl 
AND row_number = 2
AND perbblgeom >= 10;

UPDATE pluto a
SET spdist3 = sdlbl
FROM specialpurposeperorder b
WHERE a.bbl=b.bbl 
AND row_number = 3
AND perbblgeom >= 10;

DROP TABLE specialpurposeperorder;

-- set the order of special districts 
UPDATE pluto
SET spdist1 = 'CL',
  spdist2 = 'MiD'
WHERE spdist1 = 'MiD'
  AND spdist2 = 'CL';
UPDATE pluto
SET spdist1 = 'MiD',
  spdist2 = 'TA'
WHERE spdist1 = 'TA'
  AND spdist2 = 'MiD';
UPDATE pluto
SET spdist1 = '125th',
  spdist2 = 'TA'
WHERE spdist1 = 'TA'
  AND spdist2 = '125th';
UPDATE pluto
SET spdist1 = 'EC-5',
  spdist2 = 'MX-16'
WHERE spdist1 = 'MX-16'
  AND spdist2 = 'EC-5';
UPDATE pluto
SET spdist1 = 'EC-6',
  spdist2 = 'MX-16'
WHERE spdist1 = 'MX-16'
  AND spdist2 = 'EC-6';