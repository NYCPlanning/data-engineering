/** QAQC
	invalid_date_lastupdt
	invalid_date_filed
	invalid_date_statusd
	invalid_date_statusp
	invalid_date_statusr
	invalid_date_statusx
	bistest
**/


DROP TABLE IF EXISTS _INIT_QAQC;
WITH
-- identify invalid dates in input data
JOBNUMBER_INVALID_DATES AS (
    SELECT DISTINCT
        JOB_NUMBER,
        (CASE WHEN
            (is_date(DATE_LASTUPDT) AND DATE_LASTUPDT::date > '1990-01-01'::date)
            OR DATE_LASTUPDT IS NULL THEN 0
        ELSE 1 END) AS INVALID_DATE_LASTUPDT,
        (CASE WHEN
            (is_date(DATE_FILED) AND DATE_FILED::date > '1990-01-01'::date)
            OR DATE_FILED IS NULL THEN 0
        ELSE 1 END) AS INVALID_DATE_FILED,
        (CASE WHEN
            (is_date(DATE_STATUSD) AND DATE_STATUSD::date > '1990-01-01'::date)
            OR DATE_STATUSD IS NULL THEN 0
        ELSE 1 END) AS INVALID_DATE_STATUSD,
        (CASE WHEN
            (is_date(DATE_STATUSP) AND DATE_STATUSP::date > '1990-01-01'::date)
            OR DATE_STATUSP IS NULL THEN 0
        ELSE 1 END) AS INVALID_DATE_STATUSP,
        (CASE WHEN
            (is_date(DATE_STATUSR) AND DATE_STATUSR::date > '1990-01-01'::date)
            OR DATE_STATUSR IS NULL THEN 0
        ELSE 1 END) AS INVALID_DATE_STATUSR,
        (CASE WHEN
            (is_date(DATE_STATUSX) AND DATE_STATUSX::date > '1990-01-01'::date)
            OR DATE_STATUSX IS NULL THEN 0
        ELSE 1 END) AS INVALID_DATE_STATUSX
    FROM _INIT_DEVDB
),

-- identify admin jobs
JOBNUMBER_ADMIN_NOWORK AS (
    SELECT JOB_NUMBER
    FROM _INIT_DEVDB
    WHERE
        upper(JOB_DESC) LIKE '%NO WORK%'
        OR ((
            upper(JOB_DESC) LIKE '%ADMINISTRATIVE%'
            AND JOB_TYPE != 'New Building'
        )
        OR (
            upper(JOB_DESC) LIKE '%ADMINISTRATIVE%'
            AND upper(JOB_DESC) NOT LIKE '%ERECT%'
            AND JOB_TYPE = 'New Building'
        ))
        OR upper(DESC_OTHER) LIKE '%NO WORK%'
        OR ((
            upper(DESC_OTHER) LIKE '%ADMINISTRATIVE%'
            AND JOB_TYPE != 'New Building'
        )
        OR (
            upper(DESC_OTHER) LIKE '%ADMINISTRATIVE%'
            AND upper(DESC_OTHER) NOT LIKE '%ERECT%'
            AND JOB_TYPE = 'New Building'
        ))
)

SELECT
    A.*,
    (CASE
        WHEN JOB_NUMBER IN (SELECT JOB_NUMBER FROM JOBNUMBER_ADMIN_NOWORK) THEN 1
        ELSE 0
    END) AS NO_WORK_JOB
INTO _INIT_QAQC
FROM JOBNUMBER_INVALID_DATES AS A;
