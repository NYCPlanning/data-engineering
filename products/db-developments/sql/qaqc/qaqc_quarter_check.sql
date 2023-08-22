DROP TABLE IF EXISTS qaqc_quarter_check;
CREATE TABLE IF NOT EXISTS qaqc_quarter_check (
    complete_qrtr character varying,
    count int
);

INSERT INTO qaqc_quarter_check (
    SELECT
        complete_qrtr,
        COUNT(job_number) AS count
    FROM final_devdb
    WHERE complete_qrtr > '20' ||: 'VERSION_PREV'
    GROUP BY complete_qrtr
);
