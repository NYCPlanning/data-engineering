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
DROP TABLE IF EXISTS MID_devdb CASCADE;
SELECT
    _MID_devdb.*,
    STATUS_devdb.job_status,
    STATUS_devdb.job_inactive
INTO MID_devdb
FROM _MID_devdb
LEFT JOIN STATUS_devdb
ON _MID_devdb.job_number = STATUS_devdb.job_number;
CREATE INDEX MID_devdb_raw_job_number_idx ON MID_devdb_raw(job_number);
