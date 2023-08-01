-- insert unique bbls into table
INSERT INTO dcp_zoning_taxlot (
	bbl,
	boroughcode,
	taxblock,
	taxlot,
	area
	)
SELECT DISTINCT bbl, boro, block, lot, ST_Area(ST_Multi(ST_Union(geom))::geography) FROM dof_dtm GROUP BY bbl, boro, block, lot;
-- populate bbl field if it's NULL
UPDATE dcp_zoning_taxlot
SET bbl = boroughcode||lpad(taxblock, 5, '0')||lpad(taxlot, 4, '0')::text
WHERE bbl IS NULL OR length(bbl) < 10;