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
WITH
order_certtype AS (
    SELECT
        *,
        CASE
            WHEN certificatetype = 'T- TCO' THEN 2
            WHEN certificatetype = 'C- CO' THEN 1
        END AS certorder
    FROM dob_cofos
),
order_co AS (
    SELECT
        jobnum AS job_number,
        effectivedate::date AS effectivedate,
        numofdwellingunits::numeric AS units,
        certificatetype AS certtype,
        row_number() OVER (
            PARTITION BY jobnum
            ORDER BY effectivedate::date DESC, certorder ASC
        ) AS latest,
        row_number() OVER (
            PARTITION BY jobnum
            ORDER BY effectivedate::date ASC
        ) AS earliest
    FROM order_certtype
    WHERE jobnum IN (
        SELECT DISTINCT job_number
        FROM init_devdb
    )
),
draft_co AS (
    SELECT
        a.*,
        b._date_complete
    FROM (
        SELECT
            job_number,
            effectivedate AS co_latest_effectivedate,
            units AS co_latest_units,
            certtype AS co_latest_certtype
        FROM order_co
        WHERE latest = 1
    ) AS a
    LEFT JOIN (
        SELECT
            job_number,
            effectivedate AS _date_complete
        FROM order_co
        WHERE earliest = 1
    ) AS b ON a.job_number = b.job_number
)
SELECT
    job_number,
    _date_complete,
    co_latest_effectivedate,
    co_latest_units,
    co_latest_certtype
INTO co_devdb
FROM draft_co;
