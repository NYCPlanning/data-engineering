-- remove duplicate zoning values
UPDATE dcp_zoning_taxlot
SET zoningdistrict2 = NULL
WHERE zoningdistrict1 = zoningdistrict2;
UPDATE dcp_zoning_taxlot
SET zoningdistrict3 = NULL
WHERE zoningdistrict1 = zoningdistrict3;
UPDATE dcp_zoning_taxlot
SET zoningdistrict4 = NULL
WHERE zoningdistrict1 = zoningdistrict4;

UPDATE dcp_zoning_taxlot
SET zoningdistrict3 = NULL
WHERE zoningdistrict2 = zoningdistrict3;
UPDATE dcp_zoning_taxlot
SET zoningdistrict4 = NULL
WHERE zoningdistrict2 = zoningdistrict4;

UPDATE dcp_zoning_taxlot
SET zoningdistrict4 = NULL
WHERE zoningdistrict3 = zoningdistrict4;

UPDATE dcp_zoning_taxlot
SET commercialoverlay2 = NULL
WHERE commercialoverlay1 = commercialoverlay2;

UPDATE dcp_zoning_taxlot
SET specialdistrict2 = NULL
WHERE specialdistrict1 = specialdistrict2;
UPDATE dcp_zoning_taxlot
SET specialdistrict3 = NULL
WHERE specialdistrict1 = specialdistrict3;

UPDATE dcp_zoning_taxlot
SET specialdistrict2 = NULL
WHERE specialdistrict2 = specialdistrict3;
