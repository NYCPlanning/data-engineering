-- Adding on data from allocated input table
UPDATE pluto a
SET bldgclass = b.bldgclass,
	ownertype = b.ownertype,
	ownername = b.ownername,
	lotarea = b.lotarea,
	bldgarea = b.bldgarea,
	numbldgs = b.numbldgs,
	numfloors = b.numfloors,
	unitsres = b.unitsres,
	unitstotal = b.unitstotal,
	lotfront = b.lotfront,
	lotdepth = b.lotdepth,
	bldgfront = b.bldgfront,
	bldgdepth = b.bldgdepth,
	ext = b.ext,
	irrlotcode = b.irrlotcode,
	assessland = b.assessland,
	assesstot = b.assesstot,
	-- exemptland = b.exemptland,
	exempttot = b.exempttot,
	yearbuilt = b.yearbuilt,
	yearalter1 = b.yearalter1,
	yearalter2 = b.yearalter2,
	condono = b.condono,
	appbbl = b.appbbl,
	appdate = b.appdate
FROM pluto_allocated b
WHERE a.bbl = b.bbl;

--where ext is just spaces set to NULL
UPDATE pluto a
SET ext = NULL
WHERE a.ext !~ '[A-Z]';

UPDATE pluto a
SET numbldgs = count::text
FROM pluto_input_numbldgs b
WHERE a.bbl::bigint = b.bbl::bigint;