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
DROP TABLE IF EXISTS INIT_DEVDB;
SELECT DISTINCT
    B.*,
    A.GEO_BBL,
    A.GEO_BIN,
    A.GEO_ADDRESS_NUMBR,
    A.GEO_ADDRESS_STREET,
    A.GEO_ADDRESS,
    A.GEO_ZIPCODE,
    A.GEO_BORO,
    A.GEO_CD,
    A.GEO_COUNCIL,
    A.GEO_NTA2020,
    A.GEO_NTANAME2020,
    A.GEO_CB2020,
    A.GEO_CT2020,
    A.BCTCB2020,
    A.BCT2020,
    A.GEO_CDTA2020,
    A.GEO_CDTANAME2020,
    A.GEO_CSD,
    A.GEO_POLICEPRCT,
    A.GEO_FIREDIVISION,
    A.GEO_FIREBATTALION,
    A.GEO_FIRECOMPANY,
    A.GEO_SCHOOLELMNTRY,
    A.GEO_SCHOOLMIDDLE,
    A.GEO_SCHOOLSUBDIST,
    A.GEO_LATITUDE,
    A.GEO_LONGITUDE,
    A.LATITUDE,
    A.LONGITUDE,
    A.GEOM,
    A.GEOMSOURCE
INTO INIT_DEVDB
FROM SPATIAL_DEVDB AS A
LEFT JOIN _INIT_DEVDB AS B
    ON A.UID = B.UID;

-- Format dates in INIT_devdb where valid
UPDATE INIT_DEVDB
SET DATE_LASTUPDT = (CASE
    WHEN is_date(DATE_LASTUPDT) THEN DATE_LASTUPDT::date
END),
DATE_FILED = (CASE
    WHEN is_date(DATE_FILED) THEN DATE_FILED::date
END),
DATE_STATUSD = (CASE
    WHEN is_date(DATE_STATUSD) THEN DATE_STATUSD::date
END),
DATE_STATUSP = (CASE
    WHEN is_date(DATE_STATUSP) THEN DATE_STATUSP::date
END),
DATE_STATUSR = (CASE
    WHEN is_date(DATE_STATUSR) THEN DATE_STATUSR::date
END),
DATE_STATUSX = (CASE
    WHEN is_date(DATE_STATUSX) THEN DATE_STATUSX::date
END);

/*
DEDUPLICATION

For any records that share an identical job_number and BBL,
keep only the record with the most recent date_lastupdt
value and remove the older record(s). After this step, job_number
in INIT_devdb will be the uid

*/
WITH LATEST_RECORDS AS (
    SELECT
        JOB_NUMBER,
        max(DATE_LASTUPDT) AS DATE_LASTUPDT
    FROM INIT_DEVDB
    GROUP BY JOB_NUMBER
    HAVING count(*) > 1
)

DELETE FROM INIT_DEVDB A
USING LATEST_RECORDS B
WHERE
    A.JOB_NUMBER = B.JOB_NUMBER
    AND A.DATE_LASTUPDT != B.DATE_LASTUPDT;

/*
CORRECTIONS

    job_number (removal)
    bbl (removal)

*/

-- Programatically insert removals into _manual_corrections for test records andd NULL bbl
INSERT INTO _MANUAL_CORRECTIONS
(JOB_NUMBER, FIELD, REASON)
SELECT
    JOB_NUMBER,
    'remove' AS FIELD,
    'job_desc suggest this is a test record' AS REASON
FROM INIT_DEVDB
WHERE
    upper(JOB_DESC) LIKE '%BIS%TEST%'
    OR upper(JOB_DESC) LIKE '% TEST %'
    AND JOB_NUMBER NOT IN (
        SELECT DISTINCT JOB_NUMBER
        FROM _MANUAL_CORRECTIONS
        WHERE FIELD = 'remove'
    );

INSERT INTO _MANUAL_CORRECTIONS
(JOB_NUMBER, FIELD, REASON)
SELECT
    A.JOB_NUMBER,
    'remove' AS FIELD,
    'another correction set bbl from geosupport value to NULL' AS REASON
FROM INIT_DEVDB AS A
INNER JOIN _MANUAL_CORRECTIONS AS B
    ON A.JOB_NUMBER = B.JOB_NUMBER
WHERE
    B.FIELD = 'bbl'
    AND A.GEO_BBL = B.OLD_VALUE
    AND B.NEW_VALUE IS NULL;

-- Track corrections applied and not applied based on existance of job_number in INIT_devdb
WITH APPLICABLE AS (
    SELECT
        JOB_NUMBER,
        REASON
    FROM _MANUAL_CORRECTIONS
    WHERE
        JOB_NUMBER IN (SELECT JOB_NUMBER FROM INIT_DEVDB)
        AND FIELD = 'remove'
)

INSERT INTO CORRECTIONS_APPLIED
(SELECT
    JOB_NUMBER,
    'remove' AS FIELD,
    NULL AS PRE_CORR_VALUE,
    NULL AS OLD_VALUE,
    NULL AS NEW_VALUE,
    REASON
FROM APPLICABLE);

WITH NOT_APPLICABLE AS (
    SELECT
        JOB_NUMBER,
        REASON
    FROM _MANUAL_CORRECTIONS
    WHERE
        JOB_NUMBER NOT IN (SELECT JOB_NUMBER FROM INIT_DEVDB)
        AND FIELD = 'remove'
)

INSERT INTO CORRECTIONS_NOT_APPLIED
(SELECT
    JOB_NUMBER,
    'remove' AS FIELD,
    NULL AS PRE_CORR_VALUE,
    NULL AS OLD_VALUE,
    NULL AS NEW_VALUE,
    REASON
FROM NOT_APPLICABLE);

-- Remove all records with 'remove' as field with a job_number existing in INIT_devdb
DELETE FROM INIT_DEVDB A
USING _MANUAL_CORRECTIONS B
WHERE
    A.JOB_NUMBER = B.JOB_NUMBER
    AND B.FIELD = 'remove';
