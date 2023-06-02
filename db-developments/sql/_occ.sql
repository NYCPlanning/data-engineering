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

DROP TABLE IF EXISTS OCC_devdb CASCADE;
SELECT 
	job_number,
	(CASE WHEN job_type = 'New Building' THEN 'Empty Site'
		ELSE occ_translate(_occ_initial) 
	END) as occ_initial,
	(CASE WHEN job_type = 'Demolition' THEN 'Empty Site'
		ELSE occ_translate(_occ_proposed) 
	END) as occ_proposed
INTO OCC_devdb
FROM INIT_devdb;

/*
CORRECTIONS
	occ_initial
	occ_proposed
*/
CREATE INDEX OCC_devdb_job_number_idx ON OCC_devdb(job_number);
CALL apply_correction('OCC_devdb', '_manual_corrections', 'occ_initial');
CALL apply_correction('OCC_devdb', '_manual_corrections', 'occ_proposed');