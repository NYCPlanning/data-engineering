DELETE FROM dcp_zoningtaxlots.qaqc_bbl
WHERE version = :'VERSION';

INSERT INTO dcp_zoningtaxlots.qaqc_bbl (
    SELECT
        sum(CASE WHEN bblnew IS null THEN 1 ELSE 0 END) AS removed,
        sum(CASE WHEN bblold IS null THEN 1 ELSE 0 END) AS added,
        :'VERSION' AS version,
        :'VERSION_PREV' AS version_prev
    FROM (
        SELECT
            a.bbl AS bblnew,
            b.bbl AS bblold
        FROM dcp_zoningtaxlots.:"VERSION" AS a
        FULL OUTER JOIN dcp_zoningtaxlots.:"VERSION_PREV" AS b
            ON a.bbl = b.bbl
    ) AS c
);
