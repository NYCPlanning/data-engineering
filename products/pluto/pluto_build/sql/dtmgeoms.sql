-- index the bbl fields
DROP INDEX IF EXISTS dbbl_ix;
CREATE INDEX dbbl_ix
ON pluto_dtm (bbl);

-- insert bbl information and geometry of lots not in RPAD but in DTM
WITH notinpluto AS (
	SELECT b.*
	FROM pluto a
	RIGHT JOIN pluto_dtm b
	ON a.bbl = b.bbl
	WHERE a.bbl IS NULL)
INSERT INTO pluto (
	bbl,
	borocode,
	borough,
	block,
	lot,
	geom,
	plutomapid)
SELECT b.bbl,
	LEFT(b.bbl,1),
	(CASE
		WHEN LEFT(b.bbl,1) = '1' THEN 'MN'
		WHEN LEFT(b.bbl,1) = '2' THEN 'BX'
		WHEN LEFT(b.bbl,1) = '3' THEN 'BK'
		WHEN LEFT(b.bbl,1) = '4' THEN 'QN'
		WHEN LEFT(b.bbl,1) = '5' THEN 'SI'
		ELSE NULL
	END),
	trim(leading '0' FROM SUBSTRING(b.bbl,2,5)),
	trim(leading '0' FROM RIGHT(b.bbl, 4)),
	st_makevalid(st_multi(b.geom)),
	'3'
	FROM notinpluto b
;

-- DROP TABLE pluto_dtm;

-- add values in fields that cannot be NULL
UPDATE pluto
SET block = '0'
WHERE block = ''; 

UPDATE pluto
SET lot = '0'
WHERE lot = ''; 