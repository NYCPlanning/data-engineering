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
/** Indexed on the geography cast rather than plain geom: the HNY spatial match in
    _hny_match.sql compares ::geography, which a geometry index cannot serve. Without
    this the match degrades to a nested loop over the full cross product, which took
    about an hour of the 26Q2 build. **/
CREATE INDEX mid_devdb_geog_idx ON mid_devdb USING gist ((geom::geography));
