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
DROP TABLE IF EXISTS MID_DEVDB CASCADE;
SELECT
    _MID_DEVDB.*,
    STATUS_DEVDB.JOB_STATUS,
    STATUS_DEVDB.JOB_INACTIVE
INTO MID_DEVDB
FROM _MID_DEVDB
LEFT JOIN STATUS_DEVDB
    ON _MID_DEVDB.JOB_NUMBER = STATUS_DEVDB.JOB_NUMBER;
CREATE INDEX MID_DEVDB_JOB_NUMBER_IDX ON MID_DEVDB (JOB_NUMBER);
