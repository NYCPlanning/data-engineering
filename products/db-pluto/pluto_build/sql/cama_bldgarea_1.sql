-- 1 fill in area for each use from PTS
DROP INDEX IF EXISTS idx_pluto_input_cama_primebbl;
DROP INDEX IF EXISTS idx_pluto_input_cama_billingbbl;
DROP INDEX IF EXISTS idx_pluto_bbl;

CREATE INDEX idx_pluto_input_cama_primebbl ON pluto_input_cama(primebbl);
CREATE INDEX idx_pluto_input_cama_billingbbl ON pluto_input_cama(billingbbl);
CREATE INDEX idx_pluto_bbl ON pluto(bbl);

UPDATE pluto a
SET comarea = (b.officearea::numeric+b.retailarea::numeric+b.garagearea::numeric+b.storagearea::numeric+b.factoryarea::numeric+b.otherarea::numeric),
	resarea = b.residarea,
	officearea = b.officearea,
	retailarea = b.retailarea,
	garagearea = b.garagearea,
	strgearea = b.storagearea,
	factryarea = b.factoryarea,
	otherarea = b.otherarea
FROM pluto_rpad_geo b
WHERE a.bbl=b.primebbl
AND a.lot NOT LIKE '75%'
AND (b.residarea::numeric > 0
	OR b.officearea::numeric > 0
	OR b.retailarea::numeric > 0
	OR b.garagearea::numeric > 0
	OR b.storagearea::numeric > 0
	OR b.factoryarea::numeric > 0
	OR b.otherarea::numeric > 0);