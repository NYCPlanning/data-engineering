/*
DESCRIPTION:
    Merging _MID_devdb with (STATUS_devdb) to create MID_devdb
    JOIN KEY: job_number

INPUTS:

    _MID_devdb (
        * job_number,
        ...
    )

    STATUS_devdb (
        * job_number,
        job_status character varying,
        job_inactive character varying
    )


OUTPUTS:
    MID_devdb (
        * job_number,
        job_status character varying,
        complete_year text,
        complete_qrtr text,
        job_inactive character varying,
        ...
    )
*/
DROP TABLE IF EXISTS mid_devdb CASCADE;
SELECT
    _mid_devdb.*,
    status_devdb.job_status,
    status_devdb.job_inactive
INTO mid_devdb
FROM _mid_devdb
LEFT JOIN status_devdb
    ON _mid_devdb.job_number = status_devdb.job_number;
CREATE INDEX mid_devdb_job_number_idx ON mid_devdb (job_number);
