-- fill in gaps of zoning information
-- this can be deleted if overlay logic is updated to align with zonedist, spdist

UPDATE pluto
SET
    overlay1 = overlay2,
    overlay2 = NULL
WHERE overlay1 IS NULL;
