-- adding in unqique BBL information
INSERT INTO pluto (
    bbl,
    borocode,
    borough,
    block,
    lot
)
SELECT
    b.primebbl AS bbl,
    LEFT(b.primebbl, 1) AS borocode,
    CASE
        WHEN LEFT(b.primebbl, 1) = '1' THEN 'MN'
        WHEN LEFT(b.primebbl, 1) = '2' THEN 'BX'
        WHEN LEFT(b.primebbl, 1) = '3' THEN 'BK'
        WHEN LEFT(b.primebbl, 1) = '4' THEN 'QN'
        WHEN LEFT(b.primebbl, 1) = '5' THEN 'SI'
    END AS borough,
    TRIM(LEADING '0' FROM SUBSTRING(b.primebbl, 2, 5)) AS block,
    TRIM(LEADING '0' FROM RIGHT(b.primebbl, 4)) AS lot
FROM (SELECT DISTINCT primebbl FROM pluto_rpad_geo) AS b;
