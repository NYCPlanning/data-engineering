/*
DESCRIPTION:
    1. Assigning missing geoms for _GEO_devdb and create GEO_devdb
    2. Apply research corrections on (longitude, latitude, geom)

INPUTS:
    _INIT_devdb (
        * uid,
        ...
    )

    _GEO_devdb (
        * uid,
        ...
    )

    dcp_mappluto (
        * bbl,
        geom
    )

OUTPUT
    corrections_geom (
        job_number,
        field,
        old_geom,
        new_geom,
        current_latitude,
        current_longitude,
        reason,
        distance,
        null_bbl,
        in_water,
        applicable
    )

    GEO_devdb (
        * uid,
        job_number
        geo_bbl text,
        geo_bin text,
        geo_address_numbr text,
        geo_address_street text,
        geo_address text,
        geo_zipcode text,
        geo_boro text,
        geo_cd text,
        geo_council text,
        geo_csd text,
        geo_policeprct text,
        geo_latitude double precision,
        geo_longitude double precision,
        latitude double precision,
        longitude double precision,
        geom geometry,
        geomsource text
    )

IN PREVIOUS VERSION:
    geo_merge.sql
    geoaddress.sql
    geombbl.sql
    latlon.sql
    dedupe_job_number.sql
    dropmillionbin.sql
*/
DROP INDEX IF EXISTS GEO_DEVDB_GEOM_IDX;
DROP TABLE IF EXISTS GEO_DEVDB;
WITH
DRAFT AS (
    SELECT DISTINCT
        A.UID,
        A.JOB_NUMBER,
        A.BBL,
        -- a.bin,
        A.DATE_LASTUPDT,
        A.JOB_DESC,
        B.GEO_BBL,
        B.GEO_ADDRESS_NUMBR,
        B.GEO_ADDRESS_STREET,
        B.GEO_ZIPCODE,
        B.GEO_CD,
        B.GEO_COUNCIL,
        B.GEO_NTA2020,
        B.GEO_CB2020,
        B.GEO_CT2020,
        B.GEO_CDTA2020,
        B.GEO_CSD,
        B.GEO_POLICEPRCT,
        B.GEO_FIREDIVISION,
        B.GEO_FIREBATTALION,
        B.GEO_FIRECOMPANY,
        B.LATITUDE::double precision AS GEO_LATITUDE,
        B.LONGITUDE::double precision AS GEO_LONGITUDE,
        B.MODE,
        (CASE
            WHEN RIGHT(A.BIN, 6) = '000000' THEN NULL
            ELSE A.BIN
        END) AS BIN,
        (CASE
            WHEN RIGHT(B.GEO_BIN, 6) = '000000' THEN NULL
            ELSE B.GEO_BIN
        END) AS GEO_BIN,
        CONCAT(
            TRIM(B.GEO_ADDRESS_NUMBR), ' ',
            TRIM(B.GEO_ADDRESS_STREET)
        ) AS GEO_ADDRESS,
        COALESCE(REPLACE(B.GEO_BORO, '0', LEFT(B.GEO_BIN, 1)), A.BORO) AS GEO_BORO
    FROM _INIT_DEVDB AS A
    LEFT JOIN _GEO_DEVDB AS B
        ON (CASE
            WHEN SOURCE = 'bis' THEN B.UID::text
            ELSE (B.UID::integer + (SELECT MAX(_INIT_BIS_DEVDB.UID::integer) FROM _INIT_BIS_DEVDB))::text
        END)::text = A.UID::text
),

GEOM_DOB_BIN_BLDGFOOTPRINTS AS (
    SELECT DISTINCT
        A.UID,
        A.JOB_NUMBER,
        A.BBL,
        A.BIN,
        A.GEO_BBL,
        A.GEO_BIN,
        A.GEO_LATITUDE,
        A.GEO_LONGITUDE,
        ST_CENTROID(B.WKB_GEOMETRY) AS GEOM,
        (CASE
            WHEN B.WKB_GEOMETRY IS NOT NULL
                THEN 'BIN DOB buildingfootprints'
        END) AS GEOMSOURCE
    FROM DRAFT AS A
    LEFT JOIN DOITT_BUILDINGFOOTPRINTS AS B
        ON A.BIN::text = B.BIN::numeric::bigint::text
-- WHERE RIGHT(b.bin::text, 6) != '000000'
),

GEOM_GEO_BIN_BLDGFOOTPRINTS AS (
    SELECT DISTINCT
        A.UID,
        A.JOB_NUMBER,
        A.BBL,
        A.BIN,
        A.GEO_BBL,
        A.GEO_BIN,
        A.GEO_LATITUDE,
        A.GEO_LONGITUDE,
        COALESCE(A.GEOM, ST_CENTROID(B.WKB_GEOMETRY)) AS GEOM,
        (CASE
            WHEN A.GEOMSOURCE IS NOT NULL
                THEN A.GEOMSOURCE
            WHEN
                A.GEOM IS NULL
                AND B.WKB_GEOMETRY IS NOT NULL
                THEN 'BIN DCP geosupport'
        END) AS GEOMSOURCE
    FROM GEOM_DOB_BIN_BLDGFOOTPRINTS AS A
    LEFT JOIN DOITT_BUILDINGFOOTPRINTS AS B
        ON A.GEO_BIN::text = B.BIN::numeric::bigint::text
-- WHERE RIGHT(b.bin::text, 6) != '000000'
),

GEOM_GEOSUPPORT AS (
    SELECT DISTINCT
        A.UID,
        A.JOB_NUMBER,
        A.BBL,
        A.BIN,
        A.GEO_BBL,
        A.GEO_BIN,
        COALESCE(
            A.GEOM,
            ST_SETSRID(ST_POINT(A.GEO_LONGITUDE, A.GEO_LATITUDE), 4326)
        ) AS GEOM,
        (CASE
            WHEN A.GEOMSOURCE IS NOT NULL
                THEN A.GEOMSOURCE
            WHEN
                A.GEOM IS NULL
                AND A.GEO_LONGITUDE IS NOT NULL
                THEN 'Lat/Lon geosupport'
        END) AS GEOMSOURCE
    FROM GEOM_DOB_BIN_BLDGFOOTPRINTS AS A
),

GEOM_DOB_BBL_MAPPLUTO AS (
    SELECT DISTINCT
        A.UID,
        A.JOB_NUMBER,
        A.BBL,
        A.BIN,
        A.GEO_BBL,
        COALESCE(A.GEOM, ST_CENTROID(B.WKB_GEOMETRY)) AS GEOM,
        (CASE
            WHEN A.GEOMSOURCE IS NOT NULL
                THEN A.GEOMSOURCE
            WHEN
                A.GEOM IS NULL
                AND B.WKB_GEOMETRY IS NOT NULL
                THEN 'BBL DOB MapPLUTO'
        END) AS GEOMSOURCE
    FROM GEOM_GEOSUPPORT AS A
    LEFT JOIN DCP_MAPPLUTO AS B
        ON A.BBL = B.BBL::numeric::bigint::text
),

BUILDINGFOOTPRINTS_HISTORICAL AS (
    SELECT
        BIN,
        ST_UNION(WKB_GEOMETRY) AS WKB_GEOMETRY
    FROM DOITT_BUILDINGFOOTPRINTS_HISTORICAL
    GROUP BY BIN
),

GEOM_DOB_BIN_BLDGFP_HISTORICAL AS (
    SELECT DISTINCT
        A.UID,
        A.JOB_NUMBER,
        COALESCE(A.GEOM, ST_CENTROID(B.WKB_GEOMETRY)) AS GEOM,
        (CASE
            WHEN A.GEOMSOURCE IS NOT NULL
                THEN A.GEOMSOURCE
            WHEN
                A.GEOM IS NULL
                AND B.WKB_GEOMETRY IS NOT NULL
                THEN 'BIN DOB buildingfootprints (historical)'
        END) AS GEOMSOURCE
    FROM GEOM_DOB_BBL_MAPPLUTO AS A
    LEFT JOIN BUILDINGFOOTPRINTS_HISTORICAL AS B
        ON A.BIN::text = B.BIN::text
),

GEOM_DOB_LATLON AS (
    SELECT DISTINCT
        A.UID,
        A.JOB_NUMBER,
        COALESCE(
            A.GEOM,
            B.DOB_GEOM
        ) AS GEOM,
        (CASE
            WHEN A.GEOMSOURCE IS NOT NULL
                THEN A.GEOMSOURCE
            WHEN
                A.GEOM IS NULL
                AND B.DOB_GEOM IS NOT NULL
                THEN 'Lat/Lon DOB'
        END) AS GEOMSOURCE
    FROM GEOM_DOB_BIN_BLDGFP_HISTORICAL AS A
    LEFT JOIN _INIT_DEVDB AS B
        ON A.JOB_NUMBER = B.JOB_NUMBER
)

SELECT DISTINCT
    A.*,
    B.GEOM,
    B.GEOMSOURCE,
    ST_Y(B.GEOM) AS LATITUDE,
    ST_X(B.GEOM) AS LONGITUDE
INTO GEO_DEVDB
FROM DRAFT AS A
LEFT JOIN GEOM_DOB_LATLON AS B
    ON A.UID = B.UID;

-- Create index
CREATE INDEX GEO_DEVDB_GEOM_IDX ON GEO_DEVDB
USING GIST (GEOM GIST_GEOMETRY_OPS_2D);
