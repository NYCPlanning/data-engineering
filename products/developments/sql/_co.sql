/*
DESCRIPTION:
    create the certificate of occupancy table using dof_cofos

INPUTS:
    dob_cofos (
        jobnum,
        effectivedate,
        numofdwellingunits,
        certificatetype
    )

    INIT_devdb (
        job_number
    )

OUTPUTS:
    CO_devdb (
        job_number character varying,
        _date_complete date,
        co_latest_effectivedate date,
        co_latest_units numeric,
        co_latest_certtype character varying
    )

IN PREVIOUS VERSION:
    cotable.sql
    co_.sql
*/
DROP TABLE IF EXISTS co_devdb;
CREATE TABLE co_devdb AS
WITH latest_co AS (
    SELECT DISTINCT ON (jobnum)
        jobnum,
        numofdwellingunits::numeric,
        effectivedate::date,
        certificatetype
    FROM dob_cofos
    WHERE jobnum IN (SELECT job_number FROM init_devdb)
    ORDER BY
        jobnum ASC, effectivedate::date DESC, certificatetype ASC
),
earliest_co AS (
    SELECT
        jobnum,
        min(effectivedate::date) AS _date_complete
    FROM dob_cofos
    GROUP BY jobnum
)
SELECT
    latest_co.jobnum AS job_number,
    earliest_co._date_complete,
    latest_co.effectivedate AS co_latest_effectivedate,
    latest_co.numofdwellingunits AS co_latest_units,
    latest_co.certificatetype AS co_latest_certtype
FROM latest_co
INNER JOIN earliest_co ON latest_co.jobnum = earliest_co.jobnum;
