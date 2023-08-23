-- fill in gaps of zoning information
-- ie if zoningdistrict3 has a value but zoningdistrict2 is null

UPDATE pluto
SET overlay1 = overlay2,
overlay2 = NULL
WHERE overlay1 IS NULL;

UPDATE pluto
SET spdist1 = spdist2,
spdist2 = NULL
WHERE spdist1 IS NULL;
UPDATE pluto
SET spdist2 = spdist3,
spdist3 = NULL
WHERE spdist2 IS NULL;
