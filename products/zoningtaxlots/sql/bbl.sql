-- insert unique bbls into table
INSERT INTO dcp_zoning_taxlot (
    dtm_id,
    bbl,
    boroughcode,
    taxblock,
    taxlot,
    area
)
SELECT
    id,
    bbl,
    boro,
    block,
    lot,
    ST_AREA(geom) AS area
FROM dof_dtm;
-- populate bbl field if it's NULL
UPDATE dcp_zoning_taxlot
SET bbl = boroughcode || LPAD(taxblock, 5, '0') || LPAD(taxlot, 4, '0')::text
WHERE bbl IS NULL OR LENGTH(bbl) < 10;
