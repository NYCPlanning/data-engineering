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
DROP TABLE IF EXISTS CO_devdb;
WITH 
ORDER_certtype as (
	SELECT *,
	(CASE WHEN certificatetype = 'T- TCO' THEN 2
	 WHEN certificatetype = 'C- CO' THEN 1 END) as certorder
	FROM dob_cofos
),
ORDER_co as (
    SELECT
        jobnum as job_number, 
        effectivedate::date as effectivedate,
        numofdwellingunits::numeric as units, 
        certificatetype as certtype,
		ROW_NUMBER() OVER (
			PARTITION BY jobnum
			ORDER BY effectivedate::date DESC, certorder ASC) as latest,
		ROW_NUMBER() OVER (
			PARTITION BY jobnum
			ORDER BY effectivedate::date ASC) as earliest
    FROM ORDER_certtype
    WHERE jobnum IN (
        SELECT DISTINCT job_number
        FROM INIT_devdb)
),
DRAFT_co as (
	SELECT
		a.*,
		b._date_complete
	FROM (
		SELECT
			job_number, 
			effectivedate as co_latest_effectivedate,
			units as co_latest_units,
			certtype as co_latest_certtype
		FROM ORDER_co
		WHERE latest = 1
	) a
	LEFT JOIN (
		SELECT 
			job_number, 
			effectivedate as _date_complete
		FROM ORDER_co
		WHERE earliest = 1
	) b ON a.job_number = b.job_number
)
SELECT 
    job_number,
    _date_complete,
    co_latest_effectivedate,
    co_latest_units,
    co_latest_certtype
INTO CO_devdb
FROM DRAFT_co;