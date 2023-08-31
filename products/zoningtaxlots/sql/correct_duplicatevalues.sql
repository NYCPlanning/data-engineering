-- remove duplicate zoning values
UPDATE dcp_zoning_taxlot
SET commercialoverlay2 = NULL
WHERE commercialoverlay1 = commercialoverlay2;
