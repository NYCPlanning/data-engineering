-- calculate if tax lot is split by two or more zoning boundary lines and update splitzone
UPDATE pluto 
SET splitzone = 'Y'
WHERE (zonedist2 IS NOT NULL
	OR overlay2 IS NOT NULL 
	OR spdist2 IS NOT NULL);

UPDATE pluto 
SET splitzone = 'N'
WHERE splitzone IS NULL AND zonedist1 IS NOT NULL;