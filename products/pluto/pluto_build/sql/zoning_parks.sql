-- processing PARK designations
-- set the zoning district 1 value to PARK
-- where a zoning district is BALL FIELD, PLAYGROUND, or PUBLIC PLACE
UPDATE pluto a
SET zonedist1 = 'PARK'
WHERE zonedist1 = 'BALL FIELD'
	OR zonedist1 = 'PLAYGROUND'
	OR zonedist1 = 'PUBLIC PLACE';
UPDATE pluto a
SET zonedist2 = 'PARK'
WHERE zonedist2 = 'BALL FIELD'
	OR zonedist2 = 'PLAYGROUND'
	OR zonedist2 = 'PUBLIC PLACE';
UPDATE pluto a
SET zonedist3 = 'PARK'
WHERE zonedist3 = 'BALL FIELD'
	OR zonedist3 = 'PLAYGROUND'
	OR zonedist3 = 'PUBLIC PLACE';
UPDATE pluto a
SET zonedist4 = 'PARK'
WHERE zonedist4 = 'BALL FIELD'
	OR zonedist4 = 'PLAYGROUND'
	OR zonedist4 = 'PUBLIC PLACE';
-- where zoningdistrict1 = 'PARK' NULL out all other zoning information
UPDATE pluto a
SET zonedist2 = NULL,
	zonedist3 = NULL,
	zonedist4 = NULL,
	overlay1 = NULL,
	overlay2 = NULL,
	spdist1 = NULL,
	spdist2 = NULL,
	spdist3 = NULL,
	ltdheight = NULL
WHERE zonedist1 = 'PARK'
AND (zonedist2 IS NULL
	OR zonedist2 = 'PARK');