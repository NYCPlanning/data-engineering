-- drop invalid records
DELETE FROM dcp_zoning_taxlot
WHERE (boroughcode IS NULL OR boroughcode = '0')
AND (taxblock IS NULL OR taxblock = '0')
AND (taxlot IS NULL OR taxlot = '0');