-- remove duplicate zoning values
UPDATE dcp_zoning_taxlot
SET commercialoverlay2 = NULL
WHERE commercialoverlay1=commercialoverlay2;

UPDATE dcp_zoning_taxlot
SET specialdistrict2 = NULL
WHERE specialdistrict1=specialdistrict2;
UPDATE dcp_zoning_taxlot
SET specialdistrict3 = NULL
WHERE specialdistrict1=specialdistrict3;

UPDATE dcp_zoning_taxlot
SET specialdistrict2 = NULL
WHERE specialdistrict2=specialdistrict3;
