/*
DESCRIPTION:
    This script is created to assign/recode the field "date_permittd"

INPUTS:
    INIT_devdb (
        job_number
    )

    dob_permitissuance (
        jobnum,
        issuancedate
    )

OUTPUTS:
    STATUS_Q_devdb (
        job_number text,
        date_permittd date,
        permit_year text,
        permit_qrtr text
    )

*/
DROP TABLE IF EXISTS STATUS_Q_DEVDB;
WITH
STATUS_Q_CREATE AS (
    SELECT
        JOBNUM AS JOB_NUMBER,
        min(ISSUANCEDATE::date) AS DATE_PERMITTD
    FROM DOB_PERMITISSUANCE
    WHERE
        JOBDOCNUM = '01'
        AND JOBTYPE ~* 'A1|DM|NB|A2'
    GROUP BY JOBNUM
    UNION
    SELECT
        left(JOB_FILING_NUMBER, strpos(JOB_FILING_NUMBER, '-') - 1)::text AS JOB_NUMBER,
        min(ISSUED_DATE::date) AS DATE_PERMITTD
    FROM DOB_NOW_PERMITS
    GROUP BY left(JOB_FILING_NUMBER, strpos(JOB_FILING_NUMBER, '-') - 1)::text
)

SELECT
    JOB_NUMBER,
    DATE_PERMITTD,
    extract(YEAR FROM DATE_PERMITTD)::text AS PERMIT_YEAR,
    year_quarter(DATE_PERMITTD) AS PERMIT_QRTR
INTO STATUS_Q_DEVDB
FROM STATUS_Q_CREATE
