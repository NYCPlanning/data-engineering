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
DROP TABLE IF EXISTS STATUS_DEVDB CASCADE;
WITH
DRAFT_STATUS_DEVDB AS (
    SELECT
        A.JOB_NUMBER,
        A.JOB_TYPE,
        A.DATE_PERMITTD,
        A.DATE_LASTUPDT::date,
        A.CLASSA_INIT,
        A.CLASSA_PROP,
        A.CLASSA_NET,
        A.ADDRESS,
        A.OCC_PROPOSED,
        A.DATE_COMPLETE,
        A.COMPLETE_YEAR,
        A.COMPLETE_QRTR,
        CASE
            WHEN A.X_WITHDRAWAL IN ('W', 'C')
                THEN '9. Withdrawn'
            WHEN
                A.JOB_TYPE IN ('New Building', 'Alteration')
                AND A.CO_LATEST_CERTTYPE = 'T- TCO'
                AND A.CLASSA_COMPLT_PCT < 0.8
                AND A.CLASSA_NET >= 20
                THEN '4. Partially Completed Construction'
            WHEN A.DATE_COMPLETE IS NOT NULL THEN '5. Completed Construction'
            WHEN A.DATE_STATUSR IS NOT NULL THEN '3. Permitted for Construction'
            WHEN A.DATE_PERMITTD IS NOT NULL THEN '3. Permitted for Construction'
            WHEN A.DATE_STATUSP IS NOT NULL THEN '2. Approved Application'
            WHEN A.DATE_FILED IS NOT NULL THEN '1. Filed Application'
        END AS JOB_STATUS,
        GREATEST(
            DATE_LASTUPDT::date, DATE_FILED::date, DATE_STATUSD::date,
            DATE_STATUSP::date, DATE_STATUSR::date,
            DATE_STATUSX::date, DATE_COMPLETE::date, DATE_PERMITTD
        ) AS _DATE_LASTUPDT
    FROM _MID_DEVDB AS A
)

SELECT DISTINCT
    JOB_NUMBER,
    JOB_TYPE,
    JOB_STATUS,
    DATE_LASTUPDT,
    DATE_PERMITTD,
    COMPLETE_YEAR,
    COMPLETE_QRTR,
    CLASSA_INIT,
    CLASSA_PROP,
    CLASSA_NET,
    ADDRESS,
    OCC_PROPOSED,
    -- Set inactive flag
    (CASE
        -- All withdrawn jobs are inactive
        WHEN JOB_STATUS = '9. Withdrawn'
            THEN 'Inactive: Withdrawn'
        -- A date_complete indicates not inactive
        WHEN DATE_COMPLETE IS NOT NULL
            THEN NULL
        -- Jobs not (partially) complete that haven't been updated in 3 years
        WHEN
            (: 'CAPTURE_DATE'::date - _DATE_LASTUPDT) / 365 >= 3
            AND JOB_STATUS IN (
                '1. Filed Application',
                '2. Approved Application',
                '3. Permitted for Construction'
            )
            THEN 'Inactive: Stalled'
    END) AS JOB_INACTIVE
INTO STATUS_DEVDB
FROM DRAFT_STATUS_DEVDB;

-- Jobs matching with a newer, (partially) complete job get set to inactive
WITH COMPLETEJOBS AS (
    SELECT
        ADDRESS,
        JOB_TYPE,
        DATE_LASTUPDT,
        JOB_STATUS,
        CLASSA_INIT,
        CLASSA_PROP
    FROM STATUS_DEVDB
    WHERE
        CLASSA_INIT IS NOT NULL
        AND CLASSA_PROP IS NOT NULL
        AND JOB_STATUS IN ('4. Partially Completed Construction', '5. Completed Construction')
)

UPDATE STATUS_DEVDB A
SET JOB_INACTIVE = 'Inactive: Duplicate'
FROM COMPLETEJOBS AS B
WHERE
    A.JOB_STATUS IN ('1. Filed Application', '2. Approved Application', '3. Permitted for Construction')
    AND A.JOB_TYPE = B.JOB_TYPE
    AND A.ADDRESS = B.ADDRESS
    AND A.CLASSA_INIT = B.CLASSA_INIT
    AND A.CLASSA_PROP = B.CLASSA_PROP
    AND A.DATE_LASTUPDT::date < B.DATE_LASTUPDT::date;

/*
CORRECTIONS
    job_inactive
*/
CREATE INDEX STATUS_DEVDB_JOB_NUMBER_IDX ON STATUS_DEVDB (JOB_NUMBER);
CALL APPLY_CORRECTION(: 'build_schema', 'STATUS_devdb', '_manual_corrections', 'job_inactive');
