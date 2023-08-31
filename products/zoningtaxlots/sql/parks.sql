-- processing PARK designations
-- where zoningdistrict1 = 'PARK' NULL out all other zoning information
UPDATE dcp_zoning_taxlot a
SET
    zoningdistrict2 = NULL,
    zoningdistrict3 = NULL,
    zoningdistrict4 = NULL,
    commercialoverlay1 = NULL,
    commercialoverlay2 = NULL,
    specialdistrict1 = NULL,
    specialdistrict2 = NULL,
    specialdistrict3 = NULL,
    limitedheightdistrict = NULL
WHERE
    zoningdistrict1 = 'PARK'
    AND zoningdistrict2 IS NULL;
