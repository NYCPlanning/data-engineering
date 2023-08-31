-- fill in gaps of zoning information
-- ie if zoningdistrict3 has a value but zoningdistrict2 is null
UPDATE dcp_zoning_taxlot
SET
    commercialoverlay1 = commercialoverlay2,
    commercialoverlay2 = NULL
WHERE commercialoverlay1 IS NULL;
