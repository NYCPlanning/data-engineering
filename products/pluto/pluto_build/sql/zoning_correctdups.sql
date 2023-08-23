-- remove duplicate zoning values
UPDATE pluto
SET overlay2 = NULL
WHERE overlay1=overlay2;

UPDATE pluto
SET spdist2 = NULL
WHERE spdist1=spdist2;
UPDATE pluto
SET spdist3 = NULL
WHERE spdist1=spdist3;

UPDATE pluto
SET spdist2 = NULL
WHERE spdist2=spdist3;
