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
    left(b.primebbl, 1) AS borocode,
    CASE
        WHEN left(b.primebbl, 1) = '1' THEN 'MN'
        WHEN left(b.primebbl, 1) = '2' THEN 'BX'
        WHEN left(b.primebbl, 1) = '3' THEN 'BK'
        WHEN left(b.primebbl, 1) = '4' THEN 'QN'
        WHEN left(b.primebbl, 1) = '5' THEN 'SI'
    END AS borough,
    trim(LEADING '0' FROM substring(b.primebbl, 2, 5)) AS block,
    trim(LEADING '0' FROM right(b.primebbl, 4)) AS lot
FROM (SELECT DISTINCT primebbl FROM pluto_rpad_geo) AS b;
