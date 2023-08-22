/*
DESCRIPTION:
    This script creates and recodes occupancy code for devdb
	1. Assign occ_proposed and occ_initial
	2. Apply corrections on occ_proposed and occ_initial
	3. Assign occ_category
	4. Apply corrections on occ_category

INPUTS:
    INIT_devdb (
        job_number text,
        job_type text,
        _occ_initial text,
        _occ_proposed text,
    )

	lookup_occ (
		* dob_occ text,
		occ text
	)

OUTPUTS:
    OCC_devdb (
        * job_number text,
        occ_initial text,
        occ_proposed text
    )

IN PREVIOUS VERSION:
    occ_.sql
*/

DROP TABLE IF EXISTS OCC_DEVDB CASCADE;
SELECT
    JOB_NUMBER,
    (CASE
        WHEN JOB_TYPE = 'New Building' THEN 'Empty Site'
        ELSE occ_translate(_OCC_INITIAL)
    END) AS OCC_INITIAL,
    (CASE
        WHEN JOB_TYPE = 'Demolition' THEN 'Empty Site'
        ELSE occ_translate(_OCC_PROPOSED)
    END) AS OCC_PROPOSED
INTO OCC_DEVDB
FROM INIT_DEVDB;

/*
CORRECTIONS
	occ_initial
	occ_proposed
*/
CREATE INDEX OCC_DEVDB_JOB_NUMBER_IDX ON OCC_DEVDB (JOB_NUMBER);
CALL apply_correction(: 'build_schema', 'OCC_devdb', '_manual_corrections', 'occ_initial');
CALL apply_correction(: 'build_schema', 'OCC_devdb', '_manual_corrections', 'occ_proposed');
