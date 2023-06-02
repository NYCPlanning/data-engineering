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
DROP TABLE IF EXISTS STATUS_Q_devdb;
WITH 
STATUS_Q_create as (
    SELECT 
        jobnum as job_number, 
        min(issuancedate::date) as date_permittd
    FROM dob_permitissuance
    WHERE jobdocnum = '01'
    AND jobtype ~* 'A1|DM|NB|A2'
    GROUP BY jobnum
    UNION
    SELECT 
        left(job_filing_number, strpos(job_filing_number, '-') - 1)::text as job_number,
        min(issued_date::date) as date_permittd 
    FROM dob_now_permits
    GROUP BY left(job_filing_number, strpos(job_filing_number, '-') - 1)::text
) 
SELECT 
    job_number,
    date_permittd,
    extract(year from date_permittd)::text as permit_year,
    year_quarter(date_permittd) as permit_qrtr
INTO STATUS_Q_devdb
FROM STATUS_Q_create