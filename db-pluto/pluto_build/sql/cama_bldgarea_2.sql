-- 2 fill in area for each use from CAMA if not filled in from PTS
UPDATE pluto a
SET comarea = b.commercialarea,
	resarea = b.residarea,
	officearea = b.officearea,
	retailarea = b.retailarea,
	garagearea = b.garagearea,
	strgearea = b.storagearea,
	factryarea = b.factoryarea,
	otherarea = b.otherarea
FROM pluto_input_cama b
WHERE a.bbl=b.primebbl
AND b.bldgnum = '1'
AND a.lot NOT LIKE '75%'
AND (b.commercialarea::numeric > 0
	OR b.residarea::numeric > 0
	OR b.officearea::numeric > 0
	OR b.retailarea::numeric > 0
	OR b.garagearea::numeric > 0
	OR b.storagearea::numeric > 0
	OR b.factoryarea::numeric > 0
	OR b.otherarea::numeric > 0)
AND (a.comarea IS NULL
	AND a.resarea IS NULL
	AND a.officearea IS NULL
	AND a.retailarea IS NULL
	AND a.garagearea IS NULL
	AND a.strgearea IS NULL
	AND a.factryarea IS NULL
	AND a.otherarea IS NULL)
;