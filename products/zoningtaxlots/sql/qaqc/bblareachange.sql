CREATE TEMP TABLE qaqc_bblareachange AS
SELECT
    bbl,
    newarea,
    oldarea,
    areadiff,
    :'VERSION' AS version,
    :'VERSION_PREV' AS version_prev
FROM (
    SELECT DISTINCT
        a.bbl,
        a.area AS newarea,
        b.area AS oldarea,
        (a.area - b.area) AS areadiff
    FROM dcp_zoningtaxlots.:"VERSION" AS a
    INNER JOIN dcp_zoningtaxlots.:"VERSION_PREV" AS b
        ON a.bbl = b.bbl
) AS c;

\COPY qaqc_bblareachange TO PSTDOUT DELIMITER ',' CSV HEADER;
