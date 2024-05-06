-- index the bbl fields
DROP INDEX IF EXISTS dbbl_ix;
CREATE INDEX dbbl_ix
ON pluto_dtm (bbl);

-- insert bbl information and geometry of lots not in RPAD but in DTM
WITH notinpluto AS (
    SELECT a.*
    FROM pluto_dtm AS a
    LEFT JOIN pluto AS b
        ON a.bbl = b.bbl
    WHERE b.bbl IS NULL
)

INSERT INTO pluto (
    bbl,
    borocode,
    borough,
    block,
    lot,
    geom,
    plutomapid
)
SELECT
    b.bbl,
    LEFT(b.bbl, 1) AS borocode,
    CASE
        WHEN LEFT(b.bbl, 1) = '1' THEN 'MN'
        WHEN LEFT(b.bbl, 1) = '2' THEN 'BX'
        WHEN LEFT(b.bbl, 1) = '3' THEN 'BK'
        WHEN LEFT(b.bbl, 1) = '4' THEN 'QN'
        WHEN LEFT(b.bbl, 1) = '5' THEN 'SI'
    END AS borough,
    TRIM(LEADING '0' FROM SUBSTRING(b.bbl, 2, 5)) AS block,
    TRIM(LEADING '0' FROM RIGHT(b.bbl, 4)) AS lot,
    ST_MAKEVALID(ST_MULTI(b.geom)) AS geom,
    '3' AS plutomapid
FROM notinpluto AS b;

-- DROP TABLE pluto_dtm;

-- add values in fields that cannot be NULL
UPDATE pluto
SET block = '0'
WHERE block = '';

UPDATE pluto
SET lot = '0'
WHERE lot = '';
