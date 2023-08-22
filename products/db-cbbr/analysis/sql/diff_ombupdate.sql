-- what are the differences between the OMB update and the original CBBR submissions

-- records in cbbr and not in omb data
COPY (
    SELECT * FROM cbbr_submissions AS a
    LEFT JOIN (
        SELECT
            a.*,
            regid AS regidb
        FROM cbbr_ombresponse AS a
        LEFT JOIN cbbr_omblookuptable AS b
            ON a.newtrackingno = b.newtrackingno
    ) AS b
        ON upper(a.regid) = b.regidb
    WHERE b.regidb IS NULL
) TO '/prod/db-cbbr/analysis/output/cbbr_records_notinomb.csv' DELIMITER ',' CSV HEADER;

-- records in omb data and not in original cbbr data
WITH joined AS (
    SELECT * FROM cbbr_ombresponse AS a
    LEFT JOIN (
        SELECT
            a.*,
            b.regid AS regidb
        FROM cbbr_submissions AS a
        LEFT JOIN cbbr_omblookuptable AS b
            ON upper(a.regid) = b.regid
    ) AS b
        ON a.newtrackingno = b.newtrackingno
)

SELECT * FROM joined
WHERE regidb IS NULL;
