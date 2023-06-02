-- update the owner name based on IPIS
UPDATE pluto a
SET ownername = b.agency
FROM dcas_ipis b
WHERE a.bbl=b.bbl
	AND (a.ownername = ' ' 
		OR a.ownername IS NULL);


-- scratch

SELECT DISTINCT a.ownertype, a.ownername, b.agency, b.owned_leased
FROM pluto a, dcas_ipis b
WHERE a.bbl::text=b.bbl::text
AND (a.ownername = ' ' OR a.ownername IS NULLq