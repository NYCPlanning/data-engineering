/*
DESCRIPTION:
    This script is created to assign/recode the field "job_status"

INPUTS:
    _MID_devdb (
        * job_number,
        job_type character varying,
        date_lastupdt date,
        date_statusp date,
        date_permittd date,
        _job_status text,
        complete_year text,
        complete_qrtr text,
        co_latest_units numeric,
        co_latest_certtype text,
        classa_net numeric,
        address text,
        occ_proposed text
    )

    housing_input_lookup_status (
        dobstatus,
        dcpstatus
    )

OUTPUTS:
    STATUS_devdb (
        * job_number character varying,
        job_type character varying,
        job_status character varying,
        date_lastupdt date,
        date_permittd date,
        complete_year text,
        complete_qrtr text,
        classa_net numeric,
        address text,
        occ_proposed text,
        job_inactive text
    )

IN PREVIOUS VERSION:
    status.sql
    year_complete.sql
    unitscomplete.sql
*/
DROP TABLE IF EXISTS status_devdb CASCADE;
WITH
draft_status_devdb AS (
    SELECT
        a.job_number,
        a.job_type,
        CASE
            WHEN a.x_withdrawal IN ('W', 'C')
                THEN '9. Withdrawn'
            WHEN
                a.job_type IN ('New Building', 'Alteration')
                AND a.co_latest_certtype = 'T- TCO'
                AND a.classa_complt_pct < 0.8
                AND a.classa_net >= 20
                THEN '4. Partially Completed Construction'
            WHEN a.date_complete IS NOT NULL THEN '5. Completed Construction'
            WHEN a.date_statusr IS NOT NULL THEN '3. Permitted for Construction'
            WHEN a.date_permittd IS NOT NULL THEN '3. Permitted for Construction'
            WHEN a.date_statusp IS NOT NULL THEN '2. Approved Application'
            WHEN a.date_filed IS NOT NULL THEN '1. Filed Application'
        END AS job_status,
        a.date_permittd,
        a.date_lastupdt::date,
        greatest(
            date_lastupdt::date, date_filed::date, date_statusd::date,
            date_statusp::date, date_statusr::date,
            date_statusx::date, date_complete::date, date_permittd
        ) AS _date_lastupdt,
        a.classa_init,
        a.classa_prop,
        a.classa_net,
        a.address,
        a.occ_proposed,
        a.date_complete,
        a.complete_year,
        a.complete_qrtr
    FROM _mid_devdb AS a
)
SELECT DISTINCT
    job_number,
    job_type,
    job_status,
    date_lastupdt,
    date_permittd,
    complete_year,
    complete_qrtr,
    classa_init,
    classa_prop,
    classa_net,
    address,
    occ_proposed,
    -- Set inactive flag
    CASE
        -- All withdrawn jobs are inactive
        WHEN job_status = '9. Withdrawn'
            THEN 'Inactive: Withdrawn'
        -- A date_complete indicates not inactive
        WHEN date_complete IS NOT NULL
            THEN NULL
        -- Jobs not (partially) complete that haven't been updated in 3 years
        WHEN
            (:'CAPTURE_DATE'::date - _date_lastupdt) / 365 >= 3
            AND job_status IN (
                '1. Filed Application',
                '2. Approved Application',
                '3. Permitted for Construction'
            )
            THEN 'Inactive: Stalled'
    END AS job_inactive
INTO status_devdb
FROM draft_status_devdb;

-- Jobs matching with a newer, (partially) complete job get set to inactive
WITH completejobs AS (
    SELECT
        address,
        job_type,
        date_lastupdt,
        job_status,
        classa_init,
        classa_prop
    FROM status_devdb
    WHERE
        classa_init IS NOT NULL
        AND classa_prop IS NOT NULL
        AND job_status IN ('4. Partially Completed Construction', '5. Completed Construction')
)
UPDATE status_devdb a
SET job_inactive = 'Inactive: Duplicate'
FROM completejobs AS b
WHERE
    a.job_status IN ('1. Filed Application', '2. Approved Application', '3. Permitted for Construction')
    AND a.job_type = b.job_type
    AND a.address = b.address
    AND a.classa_init = b.classa_init
    AND a.classa_prop = b.classa_prop
    AND a.date_lastupdt::date < b.date_lastupdt::date;

/*
CORRECTIONS
    job_inactive
*/
CREATE INDEX status_devdb_job_number_idx ON status_devdb (job_number);
CALL apply_correction(:'build_schema', 'STATUS_devdb', '_manual_corrections', 'job_inactive');
