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
DROP TABLE IF EXISTS status_q_devdb;
WITH
status_q_create AS (
    SELECT
        jobnum AS job_number,
        min(issuancedate::date) AS date_permittd
    FROM dob_permitissuance
    WHERE
        jobdocnum = '01'
        AND jobtype ~* 'A1|DM|NB|A2'
    GROUP BY jobnum
    UNION
    SELECT
        left(job_filing_number, strpos(job_filing_number, '-') - 1)::text AS job_number,
        min(issued_date::date) AS date_permittd
    FROM dob_now_permits
    GROUP BY left(job_filing_number, strpos(job_filing_number, '-') - 1)::text
)
SELECT
    job_number,
    date_permittd,
    extract(YEAR FROM date_permittd)::text AS permit_year,
    year_quarter(date_permittd) AS permit_qrtr
INTO status_q_devdb
FROM status_q_create
