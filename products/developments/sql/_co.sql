/*
DESCRIPTION:
    Create the certificate of occupancy table from two CO sources, unioned:
      - dob_cofos:     DOB (BIS-era) COs, full history via the appended email file
      - dob_now_cofos: DOB NOW COs, sourced from Socrata, covering 2021-03 onward only
    dob_now_cofos alone lacks pre-2021 COs (that gap is why dob_cofos appends), so
    both are unioned here and the earliest/latest CO per job is derived across the
    combined set. Resolving the union at build time avoids replicating the append.

    dob_now_cofos notes:
      - Socrata display-name headers are lowercased on ingest and further
        laundered by the build DB loader (spaces -> underscores), e.g.
        c_of_o_issuance_date.
      - the job identifier is job_filing_name (9-digit BIS or letter-prefixed
        DOB NOW), NOT application_number (that is the CofO number).
      - c_of_o_issuance_date is a 12-hour text timestamp; parse with to_timestamp.
      - all rows are already "CO Issued", so no status filter is needed.

INPUTS:
    dob_cofos (
        jobnum,
        effectivedate,
        numofdwellingunits,
        certificatetype
    )

    dob_now_cofos (
        job_filing_name,
        c_of_o_issuance_date,
        number_of_dwelling_units,
        c_of_o_filing_type
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
WITH all_co AS (
    SELECT
        jobnum,
        effectivedate::date AS effectivedate,
        numofdwellingunits::numeric AS numofdwellingunits,
        certificatetype
    FROM dob_cofos
    UNION ALL
    SELECT
        job_filing_name AS jobnum,
        to_timestamp(
            nullif(trim(c_of_o_issuance_date), ''), 'MM/DD/YY HH12:MI:SS AM'
        )::date AS effectivedate,
        -- guard against non-integer junk (e.g. "..208") -> NULL rather than error
        CASE
            WHEN trim(number_of_dwelling_units) ~ '^[0-9]+$'
                THEN trim(number_of_dwelling_units)::numeric
        END AS numofdwellingunits,
        -- mirror the dob_cofos ingest mapping; unmapped filing types (incl. NULL) -> NULL
        CASE c_of_o_filing_type
            WHEN 'Final' THEN 'C- CO'
            WHEN 'Initial' THEN 'T- TCO'
            WHEN 'Renewal With Change' THEN 'T- TCO'
            WHEN 'Renewal Without Change' THEN 'T- TCO'
        END AS certificatetype
    FROM dob_now_cofos
),
latest_co AS (
    SELECT DISTINCT ON (jobnum)
        jobnum,
        numofdwellingunits,
        effectivedate,
        certificatetype
    FROM all_co
    WHERE jobnum IN (SELECT job_number FROM init_devdb)
    ORDER BY
        jobnum ASC, effectivedate DESC NULLS LAST, certificatetype ASC
),
earliest_co AS (
    SELECT
        jobnum,
        min(effectivedate) AS _date_complete
    FROM all_co
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
