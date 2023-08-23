-- create the allocated table from pluto_rpad_geo
DROP TABLE IF EXISTS pluto_allocated CASCADE;
CREATE TABLE pluto_allocated (
	bbl text,
	bldgclass text,
	ownertype  text,
	ownername text,
	lotarea text,
	bldgarea text,
	numbldgs text,
	numfloors text,
	unitsres text,
	unitstotal text,
	lotfront text,
	lotdepth text,
	bldgfront text,
	bldgdepth text,
	ext text,
	irrlotcode text,
	assessland text,
	assesstot text,
	exemptland text,
	exempttot text,
	yearbuilt text,
	yearalter1 text,
	yearalter2 text,
	condono text,
	appbbl text,
	appdate text,
	address text
);

INSERT INTO pluto_allocated (
	bbl
	)
SELECT
	b.primebbl
FROM (SELECT DISTINCT primebbl FROM pluto_rpad_geo) AS b;

-- fill in one-to-one attributes
-- for noncondo records
UPDATE pluto_allocated a
SET bldgclass = bldgcl,
	numfloors = story,
	lotfront = lfft,
	lotdepth = ldft,
	bldgfront = bfft,
	bldgdepth = bdft,
	ext = b.ext,
	condono = condo_number,
	lotarea = land_area,
	bldgarea = gross_sqft,
	yearbuilt = yrbuilt,
	yearalter1 = yralt1,
	yearalter2 = yralt2,
	ownername = owner,
	irrlotcode = irreg,
	address = concat(housenum_lo,' ',street_name),
-- set the number of buildings on a lot
-- if GeoClient returned a value then we use the value GeoClient returned
	numbldgs = (CASE 
					WHEN numberOfExistingStructuresOnLot::integer > 0 THEN numberOfExistingStructuresOnLot::integer
					ELSE bldgs
				END),
	appbbl = ap_boro||lpad(ap_block, 5, '0')||lpad(ap_lot, 4, '0'),
	appdate = ap_datef
FROM pluto_rpad_geo b
WHERE a.bbl=b.primebbl
AND b.tl NOT LIKE '75%'
AND b.condo_number IS NULL;

-- for condos get values from records where tax lot starts with 75, which indicates that this is the condo
UPDATE pluto_allocated a
SET bldgclass = bldgcl,
	numfloors = story,
	lotfront = lfft,
	lotdepth = ldft,
	bldgfront = bfft,
	bldgdepth = bdft,
	ext = b.ext,
	condono = condo_number,
	lotarea = land_area,
	yearbuilt = yrbuilt,
	yearalter1 = yralt1,
	yearalter2 = yralt2,
	ownername = owner,
	irrlotcode = irreg,
	address = concat(housenum_lo,' ',street_name),
-- set the number of buildings on a lot
-- if GeoClient returned a value then we use the value GeoClient returned
	numbldgs = (CASE 
					WHEN numberOfExistingStructuresOnLot::integer > 0 THEN numberOfExistingStructuresOnLot::integer
					ELSE bldgs
				END),
	appbbl = ap_boro||lpad(ap_block, 5, '0')||lpad(ap_lot, 4, '0'),
	appdate = ap_datef
FROM pluto_rpad_geo b
WHERE a.bbl=b.primebbl
AND b.tl LIKE '75%'
AND b.condo_number IS NOT NULL
AND b.condo_number <> '0';

-- populate the fields that where values are aggregated
-- Building area
WITH bldgareasums AS (
SELECT SUM(b.gross_sqft::numeric) AS bldgareasum, 
	primebbl
	FROM pluto_rpad_geo b
	WHERE b.tl NOT LIKE '75%'
	AND b.condo_number IS NOT NULL
	AND b.condo_number <> '0'
	GROUP BY primebbl)
UPDATE pluto_allocated a
SET bldgarea = bldgareasum
FROM bldgareasums b
WHERE a.bbl=b.primebbl;

-- unit fields
WITH primesumunits AS (
	SELECT primebbl,
	SUM(coop_apts::integer) as unitsres,
	SUM(units::integer) as unitstotal
	FROM pluto_rpad_geo b
	WHERE b.tl NOT LIKE '75%'
	GROUP BY primebbl)

UPDATE pluto_allocated a
SET unitsres = b.unitsres,
	unitstotal = b.unitstotal
FROM primesumunits b
WHERE a.bbl=b.primebbl;

-- $ fields
WITH primesums AS (
	SELECT primebbl,
	SUM(curavl_act::double precision) as assessland,
	SUM(curavt_act::double precision) as assesstot,
	-- field no longer exists
	-- SUM(curexl_act::double precision) as exemptland,
	SUM(curext_act::double precision) as exempttot
	FROM pluto_rpad_geo
	GROUP BY primebbl)

UPDATE pluto_allocated a
SET assessland = b.assessland,
	assesstot = b.assesstot,
	-- exemptland = b.exemptland,
	exempttot = b.exempttot
FROM primesums b
WHERE a.bbl=b.primebbl;

-- fill in missing attributes from supplementary table (temp solution)
UPDATE pluto_allocated a
SET lotarea = b.landsize,
	numfloors = b.story,
	yearbuilt = b.yearbuilt
FROM pluto_input_condolot_descriptiveattributes b
WHERE a.bbl = b.parid
AND a.lotarea::numeric = 0
AND a.numfloors::numeric = 0 
AND a.yearbuilt::numeric = 0;