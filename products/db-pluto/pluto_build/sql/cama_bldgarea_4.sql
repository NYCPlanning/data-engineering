-- 4 assign an area source to records that aready have bldgarea from RPAD
UPDATE pluto a
SET areasource = '2'
WHERE bldgarea::numeric <> 0 AND bldgarea IS NOT NULL;

-- 5 populate bldgarea from CAMA data
UPDATE pluto a
SET bldgarea = b.grossarea,
areasource = '7'
FROM pluto_input_cama b
WHERE a.bbl=b.primebbl
AND (bldgarea::numeric = 0 OR bldgarea IS NULL)
AND b.bldgnum = '1'
AND a.lot NOT LIKE '75%';
-- for condos
WITH primesums AS (
	SELECT billingbbl as primebbl,
	SUM(grossarea::double precision) as grossarea
	FROM pluto_input_cama
	WHERE bldgnum = '1' AND billingbbl::numeric > 0
	GROUP BY billingbbl)

UPDATE pluto a
SET bldgarea = b.grossarea,
areasource = '7'
FROM primesums b
WHERE a.bbl=b.primebbl
AND (bldgarea::numeric = 0 OR bldgarea IS NULL)
AND a.lot LIKE '75%';

-- calcualte bldgarea by multiplying bldgfront x bldgdepth X num stories
-- set area source to 5
UPDATE pluto a
SET bldgarea = a.bldgfront::numeric*a.bldgdepth::numeric*numfloors::numeric,
areasource = '5'
WHERE (a.bldgarea::numeric = 0 OR a.bldgarea IS NULL)
AND a.bldgfront::numeric <> 0
AND a.numfloors::numeric <> 0
AND a.areasource IS NULL;

-- set area source to 4 for vacant lots
-- for vacant lots and number of buildings is 0 and building floor area is 0
UPDATE pluto a
SET areasource = '4'
WHERE areasource IS NULL
	AND landuse = '11'
	AND (numbldgs::numeric = 0 OR numbldgs IS NULL)
	AND (bldgarea::numeric = 0 OR bldgarea IS NULL);

-- set area source to 0 where building area is not avialble because it's still 0 or null
UPDATE pluto a
SET areasource = '0'
WHERE a.areasource IS NULL
AND (bldgarea::numeric = 0 OR bldgarea IS NULL);