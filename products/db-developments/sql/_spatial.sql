/*
DESCRIPTION:
    1. Fill spatial boundry NULLs in GEO_devdb through spatial join
    and create SPATIAL_devdb. Note that SPATIAL_devdb is the
    consolidated table for all spatial attributes

        _GEO_devdb -> GEO_devdb
        GEO_devdb --Spatial Joins--> SPATIAL_devdb

INPUTS:
    GEO_devdb

OUTPUTS:
    SPATIAL_devdb (
        same schema as GEO_devdb
    )
*/
DROP TABLE IF EXISTS DRAFT_SPATIAL CASCADE;
CREATE TABLE DRAFT_SPATIAL AS (
    SELECT DISTINCT
        A.UID,

        -- geo_bbl
        A.GEO_ADDRESS_NUMBR,

        -- geo_bin
        A.GEO_ADDRESS_STREET,

        A.GEO_ADDRESS,
        A.GEO_LATITUDE,
        A.GEO_LONGITUDE,

        -- geo_zipcode
        A.LATITUDE,

        -- geo_boro
        A.LONGITUDE,

        -- geo_cb2020
        A.GEOM,

        -- geo_ct2020
        GEOMSOURCE,

        -- geo_csd
        (CASE WHEN
            A.GEO_BBL IS NULL
            OR A.GEO_BBL ~ '^0' OR A.GEO_BBL = ''
            OR A.MODE = 'tpad'
            THEN get_bbl(GEOM)
        ELSE A.GEO_BBL END) AS GEO_BBL,

        -- geo_cd
        (CASE WHEN (CASE WHEN
            A.GEO_BBL IS NULL
            OR A.GEO_BBL ~ '^0' OR A.GEO_BBL = ''
            OR A.MODE = 'tpad'
            THEN get_bbl(GEOM)
        ELSE A.GEO_BBL END) = get_base_bbl(GEOM)
        AND (
            A.GEO_BIN IS NULL
            OR A.GEO_BIN = ''
            OR A.GEO_BIN::NUMERIC % 1000000 = 0
        )
        OR A.MODE = 'tpad'
        AND get_base_bbl(GEOM) IS NOT NULL
            THEN get_bin(GEOM)
        ELSE A.GEO_BIN END) AS GEO_BIN,

        -- geo_council
        (CASE WHEN
            A.GEO_ZIPCODE IS NULL
            OR A.GEO_ZIPCODE = ''
            OR A.MODE = 'tpad'
            THEN get_zipcode(GEOM)
        ELSE A.GEO_ZIPCODE END) AS GEO_ZIPCODE,

        -- geo_policeprct
        (CASE WHEN
            A.GEO_BORO IS NULL
            OR A.GEO_BORO = '0'
            OR A.MODE = 'tpad'
            THEN get_boro(GEOM)::TEXT
        ELSE A.GEO_BORO END) AS GEO_BORO,

        -- geo_firedivision
        (CASE WHEN
            A.GEO_CB2020 IS NULL
            OR A.GEO_CB2020 = ''
            OR A.GEO_CT2020 = '000000'
            OR A.MODE = 'tpad'
            THEN get_cb2020(GEOM)
        ELSE A.GEO_CB2020 END) AS _GEO_CB2020,

        -- geo_firebattalion
        (CASE WHEN
            A.GEO_CT2020 IS NULL
            OR A.GEO_CT2020 = ''
            OR A.GEO_CT2020 = '000000'
            OR A.MODE = 'tpad'
            THEN get_ct2020(GEOM)
        ELSE A.GEO_CT2020 END) AS _GEO_CT2020,

        -- geo_firecompany
        (CASE WHEN
            A.GEO_CSD IS NULL
            OR A.GEO_CSD = ''
            OR A.MODE = 'tpad'
            THEN get_csd(GEOM)
        ELSE A.GEO_CSD END) AS GEO_CSD,

        (CASE WHEN
            A.GEO_CD IS NULL
            OR A.GEO_CD = ''
            OR A.MODE = 'tpad'
            THEN get_cd(GEOM)
        ELSE A.GEO_CD END) AS GEO_CD,
        (CASE WHEN
            A.GEO_COUNCIL IS NULL
            OR A.GEO_COUNCIL = ''
            OR A.MODE = 'tpad'
            THEN get_council(GEOM)
        ELSE A.GEO_COUNCIL END) AS GEO_COUNCIL,
        (CASE WHEN
            A.GEO_POLICEPRCT IS NULL
            OR A.GEO_POLICEPRCT = ''
            OR A.MODE = 'tpad'
            THEN get_policeprct(GEOM)
        ELSE A.GEO_POLICEPRCT END) AS GEO_POLICEPRCT,
        (CASE WHEN
            A.GEO_FIREDIVISION IS NULL
            OR A.GEO_FIREDIVISION = ''
            OR A.MODE = 'tpad'
            THEN get_firedivision(GEOM)
        ELSE A.GEO_FIREDIVISION END) AS GEO_FIREDIVISION,
        (CASE WHEN
            A.GEO_FIREBATTALION IS NULL
            OR A.GEO_FIREBATTALION = ''
            OR A.MODE = 'tpad'
            THEN get_firebattalion(GEOM)
        ELSE A.GEO_FIREBATTALION END) AS GEO_FIREBATTALION,
        (CASE WHEN
            A.GEO_FIRECOMPANY IS NULL
            OR A.GEO_FIRECOMPANY = ''
            OR A.MODE = 'tpad'
            THEN get_firecompany(GEOM)
        ELSE A.GEO_FIRECOMPANY END) AS GEO_FIRECOMPANY,
        get_schoolelmntry(GEOM) AS GEO_SCHOOLELMNTRY,
        get_schoolmiddle(GEOM) AS GEO_SCHOOLMIDDLE,
        get_schoolsubdist(GEOM) AS GEO_SCHOOLSUBDIST
    FROM GEO_DEVDB AS A
);
DROP INDEX IF EXISTS DRAFT_SPATIAL_UID_IDX;
CREATE INDEX DRAFT_SPATIAL_UID_IDX ON DRAFT_SPATIAL (UID);

DROP TABLE IF EXISTS CENSUS_TRACT_BLOCK CASCADE;
CREATE TABLE CENSUS_TRACT_BLOCK AS (
    SELECT DISTINCT
        UID,
        (CASE
            WHEN DRAFT_SPATIAL.GEO_BORO = '1' THEN '36061'
            WHEN DRAFT_SPATIAL.GEO_BORO = '2' THEN '36005'
            WHEN DRAFT_SPATIAL.GEO_BORO = '3' THEN '36047'
            WHEN DRAFT_SPATIAL.GEO_BORO = '4' THEN '36081'
            WHEN DRAFT_SPATIAL.GEO_BORO = '5' THEN '36085'
        END) AS FIPS,
        GEO_BORO || _GEO_CT2020 || _GEO_CB2020 AS BCTCB2020,
        GEO_BORO || _GEO_CT2020 AS BCT2020
    FROM DRAFT_SPATIAL
);

DROP INDEX IF EXISTS CENSUS_TRACT_BLOCK_UID_IDX;
DROP INDEX IF EXISTS DCP_CT2020_BOROCT2020_IDX;
CREATE INDEX CENSUS_TRACT_BLOCK_UID_IDX ON CENSUS_TRACT_BLOCK (UID);
CREATE INDEX DCP_CT2020_BOROCT2020_IDX ON DCP_CT2020 (BOROCT2020);


DROP TABLE IF EXISTS SPATIAL_DEVDB;
SELECT
    DRAFT_SPATIAL.UID,
    DRAFT_SPATIAL.GEO_BBL,
    DRAFT_SPATIAL.GEO_BIN,
    DRAFT_SPATIAL.GEO_ADDRESS_NUMBR,
    DRAFT_SPATIAL.GEO_ADDRESS_STREET,
    DRAFT_SPATIAL.GEO_ADDRESS,
    DRAFT_SPATIAL.GEO_ZIPCODE,
    DRAFT_SPATIAL.GEO_BORO,
    DRAFT_SPATIAL.GEO_CSD,
    DRAFT_SPATIAL.GEO_CD,
    DRAFT_SPATIAL.GEO_COUNCIL,
    DRAFT_SPATIAL.GEO_POLICEPRCT,
    DRAFT_SPATIAL.GEO_FIREDIVISION,
    DRAFT_SPATIAL.GEO_FIREBATTALION,
    DRAFT_SPATIAL.GEO_FIRECOMPANY,
    DRAFT_SPATIAL.GEO_SCHOOLELMNTRY,
    DRAFT_SPATIAL.GEO_SCHOOLMIDDLE,
    DRAFT_SPATIAL.GEO_SCHOOLSUBDIST,
    DRAFT_SPATIAL.GEO_LATITUDE,
    DRAFT_SPATIAL.GEO_LONGITUDE,
    DRAFT_SPATIAL.LATITUDE,
    DRAFT_SPATIAL.LONGITUDE,
    DRAFT_SPATIAL.GEOM,
    DRAFT_SPATIAL.GEOMSOURCE,
    CENSUS_TRACT_BLOCK.BCTCB2020,
    CENSUS_TRACT_BLOCK.BCT2020,
    DCP_CT2020.NTA2020 AS GEO_NTA2020,
    DCP_CT2020.NTANAME AS GEO_NTANAME2020,
    DCP_CT2020.CDTA2020 AS GEO_CDTA2020,
    DCP_CT2020.CDTANAME AS GEO_CDTANAME2020,
    CENSUS_TRACT_BLOCK.FIPS || DRAFT_SPATIAL._GEO_CT2020 || DRAFT_SPATIAL._GEO_CB2020 AS GEO_CB2020,
    CENSUS_TRACT_BLOCK.FIPS || DRAFT_SPATIAL._GEO_CT2020 AS GEO_CT2020
INTO SPATIAL_DEVDB
FROM DRAFT_SPATIAL
LEFT JOIN CENSUS_TRACT_BLOCK ON DRAFT_SPATIAL.UID = CENSUS_TRACT_BLOCK.UID
LEFT JOIN DCP_CT2020 ON CENSUS_TRACT_BLOCK.BCT2020 = BOROCT2020;

DROP TABLE IF EXISTS DRAFT_SPATIAL CASCADE;
DROP TABLE IF EXISTS CENSUS_TRACT_BLOCK CASCADE;
