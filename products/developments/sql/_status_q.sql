/*
DESCRIPTION:
    This script is created to assign/recode the field "date_permittd"

INPUTS:
    INIT_devdb (
        job_number
    )

    dob_bis_permits (
        job_number,
        job_doc_number,
        job_type,
        issuance_date
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
-- dob_bis_permits is now ingested raw; alias the cleaned column names back to
-- the names this script expects
permits AS (
    SELECT
        job_number AS jobnum,
        job_doc_number AS jobdocnum,
        job_type AS jobtype,
        issuance_date AS issuancedate
    FROM dob_bis_permits
),
status_q_create AS (
    SELECT
        jobnum AS job_number,
        min(issuancedate::date) AS date_permittd
    FROM permits
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
