UPDATE pluto a
SET ltdheight = lhlbl
FROM limitedheightperorder b
WHERE a.bbl=b.bbl 
AND perbblgeom >= 10;
DROP TABLE IF EXISTS limitedheightperorder;

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

-- add in zoning information from dcp_zoning_taxlot database
UPDATE pluto a
SET 
	zonedist1 = zoningdistrict1,
	zonedist2 = zoningdistrict2,
	zonedist3 = zoningdistrict3,
	zonedist4 = zoningdistrict4,
	overlay1 = commercialoverlay1,
	overlay2 = commercialoverlay2,
	spdist1 = specialdistrict1,
	spdist2 = specialdistrict2,
	spdist3 = specialdistrict3,
	ltdheight = limitedheightdistrict,
	zonemap = lower(zoningmapnumber),
	zmcode = zoningmapcode
FROM dcp_zoning_taxlot b
WHERE a.bbl = b.bbl;

UPDATE pluto a
SET 
	zonedist1 = zoningdistrict1,
	zonedist2 = zoningdistrict2,
	zonedist3 = zoningdistrict3,
	zonedist4 = zoningdistrict4,
	overlay1 = commercialoverlay1,
	overlay2 = commercialoverlay2,
	spdist1 = specialdistrict1,
	spdist2 = specialdistrict2,
	spdist3 = specialdistrict3,
	ltdheight = limitedheightdistrict,
	zonemap = lower(zoningmapnumber),
	zmcode = zoningmapcode
FROM dcp_zoning_taxlot b
WHERE a.appbbl = b.bbl
AND zonedist1 IS NULL;

-- calculate if tax lot is split by two or more zoning boundary lines and update splitzone
-- move code into seperate script
UPDATE pluto 
SET splitzone = 'Y'
WHERE zonedist1 IS NOT NULL
AND (zonedist2 IS NOT NULL 
	OR overlay1 IS NOT NULL 
	OR spdist1 IS NOT NULL);

UPDATE pluto 
SET splitzone = 'Y'
WHERE overlay1 IS NOT NULL
AND (zonedist1 IS NOT NULL 
	OR overlay2 IS NOT NULL 
	OR spdist1 IS NOT NULL);

UPDATE pluto 
SET splitzone = 'Y'
WHERE spdist1 IS NOT NULL
AND (zonedist1 IS NOT NULL 
	OR overlay1 IS NOT NULL 
	OR spdist2 IS NOT NULL);

UPDATE pluto 
SET splitzone = 'N'
WHERE splitzone IS NULL AND zonedist1 IS NOT NULL;

-- -- update pluto if zonedist contains two zoning districts
-- UPDATE pluto
-- SET zonedist1 = split_part(zonedist1, '/', 1),
-- 	zonedist2 = split_part(zonedist1, '/', 2)
-- WHERE zonedist1 LIKE '%/%'
-- 	AND zonedist2 IS NULL;

-- UPDATE pluto
-- SET zonedist1 = split_part(zonedist1, '/', 1),
-- 	zonedist3 = split_part(zonedist1, '/', 2)
-- WHERE zonedist1 LIKE '%/%'
-- 	AND zonedist2 IS NOT NULL
-- 	AND zonedist3 IS NULL;

-- UPDATE pluto
-- SET zonedist1 = split_part(zonedist1, '/', 1),
-- 	zonedist4 = split_part(zonedist1, '/', 2)
-- WHERE zonedist1 LIKE '%/%'
-- 	AND zonedist2 IS NOT NULL
-- 	AND zonedist3 IS NOT NULL
-- 	AND zonedist4 IS NULL;

-- UPDATE pluto
-- SET zonedist2 = split_part(zonedist2, '/', 1),
-- 	zonedist3 = split_part(zonedist2, '/', 2)
-- WHERE zonedist2 LIKE '%/%'
-- 	AND zonedist3 IS NULL;

-- UPDATE pluto
-- SET zonedist2 = split_part(zonedist2, '/', 1),
-- 	zonedist4 = split_part(zonedist2, '/', 2)
-- WHERE zonedist2 LIKE '%/%'
-- 	AND zonedist3 IS NOT NULL
-- 	AND zonedist4 IS NULL;

-- UPDATE pluto
-- SET zonedist3 = split_part(zonedist3, '/', 1),
-- 	zonedist4 = split_part(zonedist3, '/', 2)
-- WHERE zonedist3 LIKE '%/%'
-- 	AND zonedist4 IS NULL;