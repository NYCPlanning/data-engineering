/*
DESCRIPTION:

    1. Merge SPATIAL_devdb with _INIT_devdb and create INIT_devdb.

        SPATIAL_devdb + _INIT_devdb -> INIT_devdb

    2. remove records using job_number and bbl 
        in _manual_corrections 

INPUTS:
    _INIT_devdb (
        * job_number,
        ... 
    )

    SPATIAL_devdb (
        * job_number,
        ...
    )

    _INIT_qaqc (
        invalid_date_lastupdt,
	    invalid_date_filed,
	    invalid_date_statusd,
	    invalid_date_statusp,
	    invalid_date_statusr,
	    invalid_date_statusx
    )

OUTPUTS:
    
    INIT_devdb (
        _INIT_devdb.*,
        geo_bbl text,
        geo_bin text,
        geo_address_numbr text,
        geo_address_street text,
        geo_address text,
        geo_zipcode text,
        geo_boro text,
        geo_cd text,
        geo_council text,
        geo_nta2020 text,
        geo_cb2020 text,
        geo_ct2020 text,
        geo_cdta2020 text,
        geo_csd text,
        geo_policeprct text,
        geo_latitude double precision,
        geo_longitude double precision,
        latitude double precision,
        longitude double precision,
        geom geometry,
        geomsource text
    )
*/
/*
Merging spatial attribute table to the Main attribute table
*/
DROP TABLE IF EXISTS INIT_devdb;
SELECT
    distinct
    b.*,
    a.geo_bbl,
    a.geo_bin,
    a.geo_address_numbr,
    a.geo_address_street,
    a.geo_address,
    a.geo_zipcode,
    a.geo_boro,
    a.geo_cd,
    a.geo_council,
    a.geo_nta2020,
    a.geo_ntaname2020,
    a.geo_cb2020,
    a.geo_ct2020,
    a.bctcb2020,
    a.bct2020,
    a.geo_cdta2020,
    a.geo_cdtaname2020,
    a.geo_csd,
    a.geo_policeprct,
    a.geo_firedivision,
    a.geo_firebattalion,
    a.geo_firecompany,
    a.geo_schoolelmntry,
    a.geo_schoolmiddle,
    a.geo_schoolsubdist,
    a.geo_latitude,
    a.geo_longitude,
    a.latitude,
    a.longitude,
    a.geom,
    a.geomsource
INTO INIT_devdb
FROM SPATIAL_devdb a
LEFT JOIN _INIT_devdb b
ON a.uid = b.uid;

-- Format dates in INIT_devdb where valid
UPDATE INIT_devdb
SET date_lastupdt = (CASE WHEN is_date(date_lastupdt) THEN date_lastupdt::date
					ELSE NULL END),
	date_filed = (CASE WHEN is_date(date_filed) THEN date_filed::date
					ELSE NULL END),
	date_statusd = (CASE WHEN is_date(date_statusd) THEN date_statusd::date
					ELSE NULL END),
	date_statusp = (CASE WHEN is_date(date_statusp) THEN date_statusp::date
					ELSE NULL END),
	date_statusr = (CASE WHEN is_date(date_statusr) THEN date_statusr::date
					ELSE NULL END),
	date_statusx = (CASE WHEN is_date(date_statusx) THEN date_statusx::date
					ELSE NULL END);

/*
DEDUPLICATION

For any records that share an identical job_number and BBL, 
keep only the record with the most recent date_lastupdt 
value and remove the older record(s). After this step, job_number
in INIT_devdb will be the uid

*/
WITH latest_records AS (
	SELECT
        job_number, 
        MAX(date_lastupdt) AS date_lastupdt
	FROM INIT_devdb
	GROUP BY job_number
	HAVING COUNT(*)>1
)
DELETE FROM INIT_devdb a
USING latest_records b
WHERE a.job_number = b.job_number
AND a.date_lastupdt != b.date_lastupdt;

/* 
CORRECTIONS

    job_number (removal)
    bbl (removal)

*/

INSERT INTO _manual_corrections 
    (job_number, field, reason)
SELECT
    a.job_number, 
    'remove' as field,
    'another correction set bbl from geosupport value to NULL' as reason
FROM INIT_devdb a
JOIN _manual_corrections b
ON a.job_number=b.job_number
WHERE b.field = 'bbl'
AND a.geo_bbl = b.old_value
AND b.new_value IS NULL;

-- Track corrections applied and not applied based on existance of job_number in INIT_devdb
WITH applicable AS (
    SELECT
        job_number, reason
    FROM _manual_corrections
    WHERE job_number IN (SELECT job_number FROM INIT_devdb)
    AND field = 'remove'
)
INSERT INTO corrections_applied
(SELECT 
    job_number, 
    'remove' as field,
    NULL as pre_corr_value,
    NULL as old_value,
    NULL as new_value,
    reason
FROM applicable);

WITH not_applicable AS (
    SELECT
        job_number, reason
    FROM _manual_corrections
    WHERE job_number NOT IN (SELECT job_number FROM INIT_devdb)
    AND field = 'remove'
)
INSERT INTO corrections_not_applied
(SELECT 
    job_number, 
    'remove' as field,
    NULL as pre_corr_value,
    NULL as old_value,
    NULL as new_value,
    reason
FROM not_applicable);

-- Remove all records with 'remove' as field with a job_number existing in INIT_devdb
DELETE FROM INIT_devdb a
USING _manual_corrections b
WHERE a.job_number=b.job_number
AND b.field = 'remove';

