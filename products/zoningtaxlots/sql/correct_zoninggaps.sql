-- fill in gaps of zoning information
-- ie if zoningdistrict3 has a value but zoningdistrict2 is null
UPDATE dcp_zoning_taxlot
SET commercialoverlay1 = commercialoverlay2,
commercialoverlay2 = NULL
WHERE commercialoverlay1 IS NULL;

UPDATE dcp_zoning_taxlot
SET specialdistrict1 = specialdistrict2,
specialdistrict2 = NULL
WHERE specialdistrict1 IS NULL;
UPDATE dcp_zoning_taxlot
SET specialdistrict2 = specialdistrict3,
specialdistrict3 = NULL
WHERE specialdistrict2 IS NULL;
