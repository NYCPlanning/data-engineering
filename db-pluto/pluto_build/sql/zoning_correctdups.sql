-- remove duplicate zoning values
UPDATE pluto
SET zonedist2 = NULL
WHERE zonedist1=zonedist2;
UPDATE pluto
SET zonedist3 = NULL
WHERE zonedist1=zonedist3;
UPDATE pluto
SET zonedist4 = NULL
WHERE zonedist1=zonedist4;

UPDATE pluto
SET zonedist3 = NULL
WHERE zonedist2=zonedist3;
UPDATE pluto
SET zonedist4 = NULL
WHERE zonedist2=zonedist4;

UPDATE pluto
SET zonedist4 = NULL
WHERE zonedist3=zonedist4;

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