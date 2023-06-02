-- calculate how much (total area and percentage) of each lot is covered by a special purpose district
-- assign the special purpose district(s) to each tax lot
-- the order special purpose districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR more than a specified area of the lot if covered by the district
DROP TABLE IF EXISTS specialpurposeperorder;
CREATE TABLE specialpurposeperorder AS (
  WITH specialpurposeper AS (
    SELECT p.bbl,
      n.sdlbl,
      (
        ST_Area(
          CASE
            WHEN ST_CoveredBy(p.geom, n.geom) THEN p.geom
            ELSE ST_Multi(ST_Intersection(p.geom, n.geom))
          END
        )
      ) as segbblgeom,
      ST_Area(p.geom) as allbblgeom,
      (
        ST_Area(
          CASE
            WHEN ST_CoveredBy(n.geom, p.geom) THEN n.geom
            ELSE ST_Multi(ST_Intersection(n.geom, p.geom))
          END
        )
      ) as segzonegeom,
      ST_Area(n.geom) as allzonegeom
    FROM dof_dtm AS p
      INNER JOIN dcp_specialpurpose AS n ON ST_Intersects(p.geom, n.geom)
  )
  SELECT bbl,
    sdlbl,
    segbblgeom,
    (segbblgeom / allbblgeom) * 100 as perbblgeom,
    (segzonegeom / allzonegeom) * 100 as perzonegeom,
    ROW_NUMBER() OVER (
      PARTITION BY bbl
      ORDER BY segbblgeom DESC
    ) AS row_number
  FROM specialpurposeper
);
UPDATE dcp_zoning_taxlot a
SET specialdistrict1 = sdlbl
FROM specialpurposeperorder b
WHERE a.bbl::TEXT = b.bbl::TEXT
  AND row_number = 1
  AND perbblgeom >= 10;
UPDATE dcp_zoning_taxlot a
SET specialdistrict2 = sdlbl
FROM specialpurposeperorder b
WHERE a.bbl::TEXT = b.bbl::TEXT
  AND row_number = 2
  AND perbblgeom >= 10;
UPDATE dcp_zoning_taxlot a
SET specialdistrict3 = sdlbl
FROM specialpurposeperorder b
WHERE a.bbl::TEXT = b.bbl::TEXT
  AND row_number = 3
  AND perbblgeom >= 10;
--
--\copy (SELECT * FROM specialpurposeperorder ORDER BY bbl) TO '/prod/db-zoningtaxlots/zoningtaxlots_build/output/intermediate_specialpurposeperorder.csv' DELIMITER ',' CSV HEADER;
--
--DROP TABLE specialpurposeperorder;
-- set the order of special districts 
-- UPDATE dcp_zoning_taxlot
-- SET specialdistrict1 = 'CL',
--   specialdistrict2 = 'MiD'
-- WHERE specialdistrict1 = 'MiD'
--   AND specialdistrict2 = 'CL';
-- UPDATE dcp_zoning_taxlot
-- SET specialdistrict1 = 'MiD',
--   specialdistrict2 = 'TA'
-- WHERE specialdistrict1 = 'TA'
--   AND specialdistrict2 = 'MiD';
-- UPDATE dcp_zoning_taxlot
-- SET specialdistrict1 = '125th',
--   specialdistrict2 = 'TA'
-- WHERE specialdistrict1 = 'TA'
--   AND specialdistrict2 = '125th';
-- UPDATE dcp_zoning_taxlot
-- SET specialdistrict1 = 'EC-5',
--   specialdistrict2 = 'MX-16'
-- WHERE specialdistrict1 = 'MX-16'
--   AND specialdistrict2 = 'EC-5';
-- UPDATE dcp_zoning_taxlot
-- SET specialdistrict1 = 'EC-6',
--   specialdistrict2 = 'MX-16'
-- WHERE specialdistrict1 = 'MX-16'
--   AND specialdistrict2 = 'EC-6';