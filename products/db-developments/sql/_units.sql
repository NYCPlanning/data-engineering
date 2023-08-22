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
DROP TABLE IF EXISTS _UNITS_DEVDB_RAW CASCADE;
SELECT DISTINCT
    A.JOB_NUMBER,
    A.JOB_TYPE,
    A.JOB_DESC,
    B.OCC_PROPOSED,
    B.OCC_INITIAL,
    A.CLASSA_INIT,
    A.CLASSA_PROP,
    (CASE
        WHEN A.JOB_TYPE = 'New Building' THEN 0
    END) AS HOTEL_INIT,
    (CASE
        WHEN A.JOB_TYPE = 'Demolition' THEN 0
    END) AS HOTEL_PROP,
    (CASE
        WHEN A.JOB_TYPE = 'New Building' THEN 0
    END) AS OTHERB_INIT,
    (CASE
        WHEN A.JOB_TYPE = 'Demolition' THEN 0
    END) AS OTHERB_PROP
INTO _UNITS_DEVDB_RAW
FROM INIT_DEVDB AS A
LEFT JOIN OCC_DEVDB AS B
    ON A.JOB_NUMBER = B.JOB_NUMBER;

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
CREATE INDEX _UNITS_DEVDB_RAW_JOB_NUMBER_IDX ON _UNITS_DEVDB_RAW (JOB_NUMBER);
CALL apply_correction(: 'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'hotel_init', 'classa_init');
CALL apply_correction(: 'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'hotel_prop', 'classa_prop');
CALL apply_correction(: 'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'otherb_init', 'classa_init');
CALL apply_correction(: 'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'otherb_prop', 'classa_prop');
CALL apply_correction(: 'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'classa_init');
CALL apply_correction(: 'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'classa_prop');

/*
Using corrected classa, hotel, and otherb unit fields,
calculate and then manually correct resid_flag
*/
DROP TABLE IF EXISTS _UNITS_DEVDB_RESID_FLAG;
SELECT
    *,
    (CASE
        WHEN
            (HOTEL_INIT IS NOT NULL AND HOTEL_INIT != '0')
            OR (HOTEL_PROP IS NOT NULL AND HOTEL_PROP != '0')
            OR (OTHERB_INIT IS NOT NULL AND OTHERB_INIT != '0')
            OR (OTHERB_PROP IS NOT NULL AND OTHERB_PROP != '0')
            OR (CLASSA_INIT IS NOT NULL AND CLASSA_INIT != '0')
            OR (CLASSA_PROP IS NOT NULL AND CLASSA_PROP != '0')
            THEN 'Residential'
    END) AS RESID_FLAG
INTO _UNITS_DEVDB_RESID_FLAG
FROM _UNITS_DEVDB_RAW;

DROP TABLE _UNITS_DEVDB_RAW CASCADE;

/*
CORRECTIONS
	resid_flag
*/
CALL apply_correction(: 'build_schema', '_UNITS_devdb_resid_flag', '_manual_corrections', 'resid_flag');

/*
Separate A2 job types from other types of records with units
*/
DROP TABLE IF EXISTS EXPORT_A2_DEVDB;
SELECT * INTO EXPORT_A2_DEVDB
FROM _UNITS_DEVDB_RESID_FLAG WHERE JOB_TYPE = 'Alteration (A2)';

DROP TABLE IF EXISTS _UNITS_DEVDB;
SELECT
    JOB_NUMBER,
    JOB_TYPE,
    OCC_PROPOSED,
    OCC_INITIAL,
    CLASSA_INIT,
    CLASSA_PROP,
    HOTEL_INIT,
    HOTEL_PROP,
    OTHERB_INIT,
    OTHERB_PROP,
    RESID_FLAG
INTO _UNITS_DEVDB
FROM _UNITS_DEVDB_RESID_FLAG
WHERE JOB_NUMBER NOT IN (SELECT JOB_NUMBER FROM EXPORT_A2_DEVDB);

DROP TABLE _UNITS_DEVDB_RESID_FLAG CASCADE;

/*
NULL out units fields where corrected resid_flag is NULL.
ASSIGN classa_net based on corrected classa_init and classa_prop.
*/
DROP TABLE IF EXISTS UNITS_DEVDB;
WITH NULL_NONRES AS (
    SELECT
        JOB_NUMBER,
        JOB_TYPE,
        OCC_PROPOSED,
        OCC_INITIAL,
        RESID_FLAG,
        (CASE
            WHEN RESID_FLAG IS NULL THEN NULL
            ELSE CLASSA_INIT
        END) AS CLASSA_INIT,
        (CASE
            WHEN RESID_FLAG IS NULL THEN NULL
            ELSE CLASSA_PROP
        END) AS CLASSA_PROP,
        (CASE
            WHEN RESID_FLAG IS NULL THEN NULL
            ELSE HOTEL_INIT
        END) AS HOTEL_INIT,
        (CASE
            WHEN RESID_FLAG IS NULL THEN NULL
            ELSE HOTEL_PROP
        END) AS HOTEL_PROP,
        (CASE
            WHEN RESID_FLAG IS NULL THEN NULL
            ELSE OTHERB_INIT
        END) AS OTHERB_INIT,
        (CASE
            WHEN RESID_FLAG IS NULL THEN NULL
            ELSE OTHERB_PROP
        END) AS OTHERB_PROP
    FROM _UNITS_DEVDB
)

SELECT
    *,
    (CASE
        WHEN JOB_TYPE = 'Demolition'
            THEN CLASSA_INIT * -1
        WHEN JOB_TYPE = 'New Building'
            THEN CLASSA_PROP
        WHEN
            JOB_TYPE LIKE '%Alteration'
            AND CLASSA_INIT IS NOT NULL
            AND CLASSA_PROP IS NOT NULL
            THEN CLASSA_PROP - CLASSA_INIT
    END) AS CLASSA_NET
INTO UNITS_DEVDB
FROM NULL_NONRES;
