-- set the owner type code based on data from COLP
UPDATE pluto a
SET ownertype = b.ownership
FROM dcp_colp b
WHERE a.bbl::numeric = b.bbl::numeric;

-- set X as owner type
UPDATE pluto a
SET ownertype = 'X'
WHERE a.exempttot = a.assesstot
AND a.ownertype IS NULL;