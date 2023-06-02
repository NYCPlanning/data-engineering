/** QAQC
	invalid_date_lastupdt
	invalid_date_filed
	invalid_date_statusd
	invalid_date_statusp
	invalid_date_statusr
	invalid_date_statusx
	bistest
**/


DROP TABLE IF EXISTS _INIT_qaqc;
WITH
-- identify invalid dates in input data
JOBNUMBER_invalid_dates AS (
	SELECT DISTINCT job_number,
		 (CASE WHEN (is_date(date_lastupdt) AND date_lastupdt::date > '1990-01-01'::date)
		 		OR date_lastupdt IS NULL THEN 0
		 	ELSE 1 END) as invalid_date_lastupdt,
		 (CASE WHEN (is_date(date_filed) AND date_filed::date > '1990-01-01'::date)
		 		OR date_filed IS NULL THEN 0
		 	ELSE 1 END) as invalid_date_filed,
		 (CASE WHEN (is_date(date_statusd) AND date_statusd::date > '1990-01-01'::date)
		 		OR date_statusd IS NULL THEN 0
		 	ELSE 1 END) as invalid_date_statusd,
		 (CASE WHEN (is_date(date_statusp) AND date_statusp::date > '1990-01-01'::date)
		 		OR date_statusp IS NULL THEN 0
		 	ELSE 1 END) as invalid_date_statusp,
		 (CASE WHEN (is_date(date_statusr) AND date_statusr::date > '1990-01-01'::date)
		 		OR date_statusr IS NULL THEN 0
		 	ELSE 1 END) as invalid_date_statusr,
		 (CASE WHEN (is_date(date_statusx) AND date_statusx::date > '1990-01-01'::date)
		 		OR date_statusx IS NULL THEN 0
		 	ELSE 1 END) as invalid_date_statusx
		FROM _INIT_devdb ),

-- identify admin jobs
JOBNUMBER_admin_nowork as (
	SELECT job_number
	FROM _INIT_devdb
	WHERE upper(job_desc) LIKE '%NO WORK%'
	OR ((upper(job_desc) LIKE '%ADMINISTRATIVE%'
		AND job_type <> 'New Building')
	OR (upper(job_desc) LIKE '%ADMINISTRATIVE%'
		AND upper(job_desc) NOT LIKE '%ERECT%'
		AND job_type = 'New Building'))
	OR upper(desc_other) LIKE '%NO WORK%'
	OR ((upper(desc_other) LIKE '%ADMINISTRATIVE%'
		AND job_type <> 'New Building')
	OR (upper(desc_other) LIKE '%ADMINISTRATIVE%'
		AND upper(desc_other) NOT LIKE '%ERECT%'
		AND job_type = 'New Building'))
)

SELECT a.*,
	(CASE 
	 	WHEN job_number IN (SELECT job_number FROM JOBNUMBER_admin_nowork) THEN 1
	 	ELSE 0
	END) as no_work_job
INTO _INIT_qaqc
FROM JOBNUMBER_invalid_dates a
;