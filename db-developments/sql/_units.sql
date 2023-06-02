
/*
DESCRIPTION:
    This script assigns units fields for devdb
	1. Assign _classa_init and classa_prop
	2. Apply corrections to _classa_init and classa_prop
	3. Assign _classa_net
INPUTS: 
	INIT_devdb (
		job_number text,
		job_type text,
		classa_init numeric,
		classa_prop numeric
	)
	OCC_devdb (
		job_number text,
		occ_initial text,
		occ_proposed text
	)
OUTPUTS:
	UNITS_devdb (
		job_number text, 
		classa_init numeric,
		classa_prop numeric,
		hotel_init numeric,
		hotel_prop numeric,
		otherb_init numeric,
		otherb_prop numeric,
		classa_net numeric,
		resid_flag text
	)
IN PREVIOUS VERSION: 
    units_.sql
	units_net.sql
*/
DROP TABLE IF EXISTS _UNITS_devdb_raw CASCADE;
SELECT DISTINCT
	a.job_number,
	a.job_type,
	a.job_desc,
	b.occ_proposed,
	b.occ_initial,
	a.classa_init,
	a.classa_prop,
	(CASE
		WHEN a.job_type = 'New Building' THEN 0
		ELSE NULL
	END) as hotel_init,
	(CASE
		WHEN a.job_type = 'Demolition' THEN 0
		ELSE NULL
	END) as hotel_prop,
	(CASE
		WHEN a.job_type = 'New Building' THEN 0
		ELSE NULL
	END) as otherb_init,
	(CASE
		WHEN a.job_type = 'Demolition' THEN 0
		ELSE NULL
	END) as otherb_prop
INTO _UNITS_devdb_raw
FROM INIT_devdb a
LEFT JOIN OCC_devdb b
ON a.job_number = b.job_number;

/*
CORRECTIONS
	hotel_init
	hotel_prop
	otherb_init
	otherb_prop
	classa_init
	classa_prop
Note that hotel/otherb corrections match old_value with
the associated classa field. As a result, these corrections
get applied prior to the classa corrections.
*/
CREATE INDEX _UNITS_devdb_raw_job_number_idx ON _UNITS_devdb_raw(job_number);
CALL apply_correction('_UNITS_devdb_raw', '_manual_corrections', 'hotel_init', 'classa_init');
CALL apply_correction('_UNITS_devdb_raw', '_manual_corrections', 'hotel_prop', 'classa_prop');
CALL apply_correction('_UNITS_devdb_raw', '_manual_corrections', 'otherb_init', 'classa_init');
CALL apply_correction('_UNITS_devdb_raw', '_manual_corrections', 'otherb_prop', 'classa_prop');
CALL apply_correction('_UNITS_devdb_raw', '_manual_corrections', 'classa_init');
CALL apply_correction('_UNITS_devdb_raw', '_manual_corrections', 'classa_prop');

/*
Using corrected classa, hotel, and otherb unit fields, 
calculate and then manually correct resid_flag
*/
DROP TABLE IF EXISTS _UNITS_devdb_resid_flag;
SELECT
	*,
	(CASE 
		WHEN (hotel_init IS NOT NULL AND hotel_init <> '0')
			OR (hotel_prop IS NOT NULL AND hotel_prop <> '0')
			OR (otherb_init IS NOT NULL AND otherb_init <> '0')
			OR (otherb_prop IS NOT NULL AND otherb_prop <> '0')
			OR (classa_init IS NOT NULL AND classa_init <> '0')
			OR (classa_prop IS NOT NULL AND classa_prop <> '0')
			THEN 'Residential' 
	END) as resid_flag
INTO _UNITS_devdb_resid_flag
FROM _UNITS_devdb_raw
;

DROP TABLE _UNITS_devdb_raw CASCADE;

/*
CORRECTIONS
	resid_flag
*/
CALL apply_correction('_UNITS_devdb_resid_flag', '_manual_corrections', 'resid_flag');

/*
Separate A2 job types from other types of records with units 
*/
DROP TABLE IF EXISTS EXPORT_A2_devdb;
SELECT * INTO EXPORT_A2_devdb 
FROM _UNITS_devdb_resid_flag WHERE job_type = 'Alteration (A2)';

DROP TABLE IF EXISTS _UNITS_devdb;
SELECT 
	job_number,
	job_type,
	occ_proposed,
	occ_initial,
	classa_init,
	classa_prop,
	hotel_init,
	hotel_prop,
	otherb_init,
	otherb_prop, 
	resid_flag
 INTO _UNITS_devdb 
FROM _UNITS_devdb_resid_flag 
WHERE job_number NOT IN  (SELECT job_number FROM EXPORT_A2_devdb);

DROP TABLE _UNITS_devdb_resid_flag CASCADE;

/*
NULL out units fields where corrected resid_flag is NULL.
ASSIGN classa_net based on corrected classa_init and classa_prop.
*/
DROP TABLE IF EXISTS UNITS_devdb;
WITH NULL_nonres AS (
	SELECT
		job_number,
		job_type,
		occ_proposed,
		occ_initial,
		(CASE
			WHEN resid_flag IS NULL THEN NULL
			ELSE classa_init
		END) as classa_init,
		(CASE
			WHEN resid_flag IS NULL THEN NULL
			ELSE classa_prop
		END) as classa_prop,
		(CASE
			WHEN resid_flag IS NULL THEN NULL
			ELSE hotel_init
		END) as hotel_init,
		(CASE
			WHEN resid_flag IS NULL THEN NULL
			ELSE hotel_prop
		END) as hotel_prop,
		(CASE
			WHEN resid_flag IS NULL THEN NULL
			ELSE otherb_init
		END) as otherb_init,
		(CASE
			WHEN resid_flag IS NULL THEN NULL
			ELSE otherb_prop
		END) as otherb_prop,
		resid_flag
	FROM _UNITS_devdb
)
SELECT
	*,
	(CASE
		WHEN job_type = 'Demolition' 
			THEN classa_init * -1
		WHEN job_type = 'New Building' 
			THEN classa_prop
		WHEN job_type LIKE '%Alteration' 
			AND classa_init IS NOT NULL 
			AND classa_prop IS NOT NULL 
			THEN classa_prop - classa_init
		ELSE NULL
	END) as classa_net
INTO UNITS_devdb
FROM NULL_nonres;