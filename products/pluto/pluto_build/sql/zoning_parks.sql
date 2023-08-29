-- where zoningdistrict1 = 'PARK' NULL out all other zoning information
UPDATE pluto a
SET
    overlay1 = NULL,
    overlay2 = NULL,
    spdist1 = NULL,
    spdist2 = NULL,
    spdist3 = NULL,
    ltdheight = NULL
WHERE zonedist1 = 'PARK' AND zonedist2 IS NULL;
