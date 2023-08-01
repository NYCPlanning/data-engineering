-- Setting the landuse of the lot based on the building class
-- uses the pluto_input_landuse_bldgclass lookup table
UPDATE pluto a
SET landuse = b.landuse
FROM pluto_input_landuse_bldgclass b
WHERE a.bldgclass = b.bldgclass;

-- set area source to 4 for vacant lots
-- for vacant lots and number of buildings is 0 and building floor area is 0
UPDATE pluto a
SET areasource = '4'
WHERE (areasource IS NULL OR areasource = '0')
	AND landuse = '11'
	AND numbldgs::numeric = 0
	AND (bldgarea::numeric = 0 OR bldgarea IS NULL);