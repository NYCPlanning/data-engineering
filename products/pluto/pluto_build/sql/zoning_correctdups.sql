-- remove duplicate zoning values
-- this can be deleted if overlay logic is updated to align with zonedist, spdist
UPDATE pluto
SET overlay2 = NULL
WHERE overlay1=overlay2;
