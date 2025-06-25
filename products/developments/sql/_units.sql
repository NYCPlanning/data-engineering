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
DROP TABLE IF EXISTS _units_devdb_raw CASCADE;
SELECT DISTINCT
    a.job_number,
    a.job_type,
    a.job_desc,
    a.work_types,
    a.address,
    a.boro,
    a.bin,
    a.bbl,
    a.geo_zipcode,
    a.geo_cdtaname2020,
    a.geo_latitude,
    a.geo_longitude,
    a.datasource,
    b.occ_proposed,
    b.occ_initial,
    a.classa_init,
    a.classa_prop,
    CASE
        WHEN a.job_type = 'New Building' THEN 0
    END AS hotel_init,
    CASE
        WHEN a.job_type = 'Demolition' THEN 0
    END AS hotel_prop,
    CASE
        WHEN a.job_type = 'New Building' THEN 0
    END AS otherb_init,
    CASE
        WHEN a.job_type = 'Demolition' THEN 0
    END AS otherb_prop
INTO _units_devdb_raw
FROM init_devdb AS a
LEFT JOIN occ_devdb AS b
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
CREATE INDEX _units_devdb_raw_job_number_idx ON _units_devdb_raw (job_number);
CALL apply_correction(:'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'hotel_init', 'classa_init');
CALL apply_correction(:'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'hotel_prop', 'classa_prop');
CALL apply_correction(:'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'otherb_init', 'classa_init');
CALL apply_correction(:'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'otherb_prop', 'classa_prop');
CALL apply_correction(:'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'classa_init');
CALL apply_correction(:'build_schema', '_UNITS_devdb_raw', '_manual_corrections', 'classa_prop');

/*
Using corrected classa, hotel, and otherb unit fields,
calculate and then manually correct resid_flag
*/
DROP TABLE IF EXISTS _units_devdb_resid_flag;
SELECT
    *,
    CASE
        WHEN
            (hotel_init IS NOT NULL AND hotel_init != '0')
            OR (hotel_prop IS NOT NULL AND hotel_prop != '0')
            OR (otherb_init IS NOT NULL AND otherb_init != '0')
            OR (otherb_prop IS NOT NULL AND otherb_prop != '0')
            OR (classa_init IS NOT NULL AND classa_init != '0')
            OR (classa_prop IS NOT NULL AND classa_prop != '0')
            THEN 'Residential'
    END AS resid_flag
INTO _units_devdb_resid_flag
FROM _units_devdb_raw;

/*
CORRECTIONS
	resid_flag
*/
CALL apply_correction(:'build_schema', '_UNITS_devdb_resid_flag', '_manual_corrections', 'resid_flag');

/*
Determine all A2 job details
*/
DROP TABLE IF EXISTS _units_devdb_a2_details;
SELECT
    _units_devdb_resid_flag.*,
    job_type IN ('Alteration (A2)', 'Alteration (Non-CO)') AS presumed_a2_alteration,
    datasource = 'bis' OR (
        job_desc IS NOT NULL
        AND lower(job_desc) LIKE '%combin%'
        AND lower(job_desc) NOT LIKE '%sprinkler%'
    ) AS meets_bis_a2_inclusion_rules,
    a2_corrections.job_number IS NOT NULL AS include_a2_record,
    a2_corrections.reason AS a2_inclusion_reason
INTO _units_devdb_a2_details
FROM _units_devdb_resid_flag
LEFT JOIN a2_corrections
    ON _units_devdb_resid_flag.job_number = a2_corrections.job_number;


DROP TABLE IF EXISTS export_a2_devdb;
SELECT * INTO export_a2_devdb
FROM _units_devdb_a2_details
WHERE presumed_a2_alteration;

DROP TABLE IF EXISTS _units_devdb;
SELECT
    job_number,
    job_type,
    work_types,
    occ_proposed,
    occ_initial,
    classa_init,
    classa_prop,
    hotel_init,
    hotel_prop,
    otherb_init,
    otherb_prop,
    resid_flag
INTO _units_devdb
FROM _units_devdb_a2_details
WHERE (NOT presumed_a2_alteration) OR include_a2_record;

/*
NULL out units fields where corrected resid_flag is NULL.
ASSIGN classa_net based on corrected classa_init and classa_prop.
*/
DROP TABLE IF EXISTS units_devdb;
WITH null_nonres AS (
    SELECT
        job_number,
        job_type,
        work_types,
        occ_proposed,
        occ_initial,
        CASE
            WHEN resid_flag IS NULL THEN NULL
            ELSE classa_init
        END AS classa_init,
        CASE
            WHEN resid_flag IS NULL THEN NULL
            ELSE classa_prop
        END AS classa_prop,
        CASE
            WHEN resid_flag IS NULL THEN NULL
            ELSE hotel_init
        END AS hotel_init,
        CASE
            WHEN resid_flag IS NULL THEN NULL
            ELSE hotel_prop
        END AS hotel_prop,
        CASE
            WHEN resid_flag IS NULL THEN NULL
            ELSE otherb_init
        END AS otherb_init,
        CASE
            WHEN resid_flag IS NULL THEN NULL
            ELSE otherb_prop
        END AS otherb_prop,
        resid_flag
    FROM _units_devdb
)
SELECT
    *,
    CASE
        WHEN job_type = 'Demolition'
            THEN classa_init * -1
        WHEN job_type = 'New Building'
            THEN classa_prop
        WHEN
            job_type LIKE '%Alteration'
            AND classa_init IS NOT NULL
            AND classa_prop IS NOT NULL
            THEN classa_prop - classa_init
    END AS classa_net
INTO units_devdb
FROM null_nonres;
