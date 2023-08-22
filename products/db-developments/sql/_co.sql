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
DROP TABLE IF EXISTS CO_DEVDB;
WITH ORDER_CERTTYPE AS (
    SELECT
        *,
        (CASE
            WHEN CERTIFICATETYPE = 'T- TCO' THEN 2
            WHEN CERTIFICATETYPE = 'C- CO' THEN 1
        END) AS CERTORDER
    FROM DOB_COFOS
),

DRAFT_CO AS (
    SELECT
        A.*,
        B._DATE_COMPLETE
    FROM (
        SELECT
            JOB_NUMBER,
            EFFECTIVEDATE AS CO_LATEST_EFFECTIVEDATE,
            UNITS AS CO_LATEST_UNITS,
            CERTTYPE AS CO_LATEST_CERTTYPE
        FROM ORDER_CO
        WHERE LATEST = 1
    ) AS A
    LEFT JOIN B ON A.JOB_NUMBER = B.JOB_NUMBER
),

B AS (
    SELECT
        JOB_NUMBER,
        EFFECTIVEDATE AS _DATE_COMPLETE
    FROM ORDER_CO
    WHERE EARLIEST = 1
)

SELECT
    JOB_NUMBER,
    _DATE_COMPLETE,
    CO_LATEST_EFFECTIVEDATE,
    CO_LATEST_UNITS,
    CO_LATEST_CERTTYPE
INTO CO_DEVDB
FROM DRAFT_CO;
