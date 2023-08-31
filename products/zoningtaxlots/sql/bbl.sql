-- insert unique bbls into table
INSERT INTO dcp_zoning_taxlot (
    bbl,
    boroughcode,
    taxblock,
    taxlot,
    area
)
SELECT
    bbl,
    boro,
    block,
    lot,
    ST_AREA(ST_MULTI(ST_UNION(geom))::geography) AS area
FROM dof_dtm
GROUP BY bbl, boro, block, lot;
-- populate bbl field if it's NULL
UPDATE dcp_zoning_taxlot
SET bbl = boroughcode || LPAD(taxblock, 5, '0') || LPAD(taxlot, 4, '0')::text
WHERE bbl IS NULL OR LENGTH(bbl) < 10;
