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
DROP TABLE IF EXISTS DRAFT_spatial CASCADE;
CREATE TABLE DRAFT_spatial AS (
    SELECT
        distinct
        a.uid,

        -- geo_bbl
        (CASE WHEN a.geo_bbl IS NULL
            OR a.geo_bbl ~ '^0' OR a.geo_bbl = ''
            OR a.mode = 'tpad'
            THEN get_bbl(geom)
        ELSE a.geo_bbl END) as geo_bbl, 

        -- geo_bin
        (CASE WHEN (CASE WHEN a.geo_bbl IS NULL
            OR a.geo_bbl ~ '^0' OR a.geo_bbl = ''
            OR a.mode = 'tpad'
            THEN get_bbl(geom)
        ELSE a.geo_bbl END) = get_base_bbl(geom)
            AND (a.geo_bin IS NULL 
                OR a.geo_bin = '' 
                OR a.geo_bin::NUMERIC%1000000=0)
                OR a.mode = 'tpad'
            AND get_base_bbl(geom) IS NOT NULL
            THEN get_bin(geom)
        ELSE a.geo_bin END) as geo_bin,

        a.geo_address_numbr,
        a.geo_address_street,
        a.geo_address,

        -- geo_zipcode
        (CASE WHEN a.geo_zipcode IS NULL 
            OR a.geo_zipcode = ''
            OR a.mode = 'tpad'
            THEN get_zipcode(geom)
        ELSE a.geo_zipcode END) as geo_zipcode, 

        -- geo_boro
        (CASE WHEN a.geo_boro IS NULL 
            OR a.geo_boro = '0'
            OR a.mode = 'tpad'
            THEN get_boro(geom)::text
        ELSE a.geo_boro END) as geo_boro,

        -- geo_cb2010
        (CASE WHEN a.geo_cb2010 IS NULL 
            OR a.geo_cb2010 = '' 
            OR a.geo_ct2010 = '000000' 
            OR a.mode = 'tpad'
            THEN get_cb2010(geom)
        ELSE a.geo_cb2010 END) as _geo_cb2010, 

        -- geo_ct2010
    (CASE WHEN a.geo_ct2010 IS NULL 
            OR a.geo_ct2010 = '' 
            OR a.geo_ct2010 = '000000'
            OR a.mode = 'tpad' 
            THEN get_ct2010(geom)
        ELSE a.geo_ct2010 END) as _geo_ct2010, 

        -- geo_cb2010
        (CASE WHEN a.geo_cb2020 IS NULL 
            OR a.geo_cb2020 = '' 
            OR a.geo_ct2020 = '000000' 
            OR a.mode = 'tpad'
            THEN get_cb2020(geom)
        ELSE a.geo_cb2020 END) as _geo_cb2020, 

        -- geo_ct2010
    (CASE WHEN a.geo_ct2020 IS NULL 
            OR a.geo_ct2020 = '' 
            OR a.geo_ct2020 = '000000'
            OR a.mode = 'tpad' 
            THEN get_ct2020(geom)
        ELSE a.geo_ct2020 END) as _geo_ct2020, 
    
        -- geo_csd
        (CASE WHEN a.geo_csd IS NULL 
            OR a.geo_csd = '' 
            OR a.mode = 'tpad'
            THEN get_csd(geom)
        ELSE a.geo_csd END) as geo_csd, 

        -- geo_cd
        (CASE WHEN a.geo_cd IS NULL 
            OR a.geo_cd = '' 
            OR a.mode = 'tpad'
            THEN get_cd(geom)
        ELSE a.geo_cd END) as geo_cd, 

        -- geo_council
        (CASE WHEN a.geo_council IS NULL 
            OR a.geo_council = '' 
            OR a.mode = 'tpad'
            THEN get_council(geom)
        ELSE a.geo_council END) as geo_council, 

        -- geo_policeprct
        (CASE WHEN a.geo_policeprct IS NULL 
            OR a.geo_policeprct = '' 
            OR a.mode = 'tpad'
            THEN get_policeprct(geom)
        ELSE a.geo_policeprct END) as geo_policeprct, 

        -- geo_firedivision
        (CASE WHEN a.geo_firedivision IS NULL 
            OR a.geo_firedivision = '' 
            OR a.mode = 'tpad'
            THEN get_firedivision(geom)
        ELSE a.geo_firedivision END) as geo_firedivision, 

        -- geo_firebattalion
        (CASE WHEN a.geo_firebattalion IS NULL 
            OR a.geo_firebattalion = '' 
            OR a.mode = 'tpad'
            THEN get_firebattalion(geom)
        ELSE a.geo_firebattalion END) as geo_firebattalion, 

        -- geo_firecompany
        (CASE WHEN a.geo_firecompany IS NULL 
            OR a.geo_firecompany = '' 
            OR a.mode = 'tpad'
            THEN get_firecompany(geom)
        ELSE a.geo_firecompany END) as geo_firecompany, 

        get_schoolelmntry(geom) as geo_schoolelmntry,
        get_schoolmiddle(geom) as geo_schoolmiddle,
        get_schoolsubdist(geom) as geo_schoolsubdist,
        a.geo_latitude,
        a.geo_longitude,
        a.latitude,
        a.longitude,
        a.geom,
        geomsource
    FROM GEO_devdb a
);
DROP INDEX IF EXISTS DRAFT_spatial_uid_idx;
CREATE INDEX DRAFT_spatial_uid_idx ON DRAFT_spatial(uid);

DROP TABLE IF EXISTS CENSUS_TRACT_BLOCK CASCADE;
CREATE TABLE CENSUS_TRACT_BLOCK AS (
    SELECT
        distinct uid,
        (CASE
            WHEN DRAFT_spatial.geo_boro = '1' THEN '36061'
            WHEN DRAFT_spatial.geo_boro = '2' THEN '36005'
            WHEN DRAFT_spatial.geo_boro = '3' THEN '36047'
            WHEN DRAFT_spatial.geo_boro = '4' THEN '36081'
            WHEN DRAFT_spatial.geo_boro = '5' THEN '36085'
        END) as fips,
        geo_boro||_geo_ct2010||_geo_cb2010 as bctcb2010,
        geo_boro||_geo_ct2020||_geo_cb2020 as bctcb2020,
        geo_boro||_geo_ct2010 as bct2010,
        geo_boro||_geo_ct2020 as bct2020
    FROM DRAFT_spatial
);

DROP INDEX IF EXISTS CENSUS_TRACT_BLOCK_uid_idx;
DROP INDEX IF EXISTS dcp_ct2020_boroct2020_idx;
DROP INDEX IF EXISTS dcp_ct2010_boroct2010_idx;
CREATE INDEX CENSUS_TRACT_BLOCK_uid_idx ON CENSUS_TRACT_BLOCK(uid);
CREATE INDEX dcp_ct2020_boroct2020_idx ON dcp_ct2020(boroct2020);
CREATE INDEX dcp_ct2010_boroct2010_idx ON dcp_ct2010(boroct2010);


DROP TABLE IF EXISTS SPATIAL_devdb;
SELECT
    DRAFT_spatial.uid,
    DRAFT_spatial.geo_bbl,
    DRAFT_spatial.geo_bin,
    DRAFT_spatial.geo_address_numbr,
    DRAFT_spatial.geo_address_street,
    DRAFT_spatial.geo_address,
    DRAFT_spatial.geo_zipcode,
    DRAFT_spatial.geo_boro,
    DRAFT_spatial.geo_csd,
    DRAFT_spatial.geo_cd,
    DRAFT_spatial.geo_council,
    DRAFT_spatial.geo_policeprct,
    DRAFT_spatial.geo_firedivision,
    DRAFT_spatial.geo_firebattalion,
    DRAFT_spatial.geo_firecompany,
    DRAFT_spatial.geo_schoolelmntry,
    DRAFT_spatial.geo_schoolmiddle,
    DRAFT_spatial.geo_schoolsubdist,
    DRAFT_spatial.geo_latitude,
    DRAFT_spatial.geo_longitude,
    DRAFT_spatial.latitude,
    DRAFT_spatial.longitude,
    DRAFT_spatial.geom,
    DRAFT_spatial.geomsource,
    CENSUS_TRACT_BLOCK.fips||DRAFT_spatial._geo_ct2010||DRAFT_spatial._geo_cb2010 as geo_cb2010,
    CENSUS_TRACT_BLOCK.fips||DRAFT_spatial._geo_ct2010 as geo_ct2010,
    CENSUS_TRACT_BLOCK.bctcb2010,
    CENSUS_TRACT_BLOCK.bct2010,
    CENSUS_TRACT_BLOCK.fips||DRAFT_spatial._geo_ct2020||DRAFT_spatial._geo_cb2020 as geo_cb2020,
    CENSUS_TRACT_BLOCK.fips||DRAFT_spatial._geo_ct2020 as geo_ct2020,
    CENSUS_TRACT_BLOCK.bctcb2020,
    CENSUS_TRACT_BLOCK.bct2020,
    dcp_ct2010.ntacode as geo_nta2010,
    dcp_ct2010.ntaname as geo_ntaname2010,
    dcp_ct2020.nta2020 as geo_nta2020,
    dcp_ct2020.ntaname as geo_ntaname2020,
    dcp_ct2020.cdta2020 as geo_cdta2020,
    dcp_ct2020.cdtaname as geo_cdtaname2020
INTO SPATIAL_devdb
FROM DRAFT_spatial
LEFT JOIN CENSUS_TRACT_BLOCK ON DRAFT_spatial.uid = CENSUS_TRACT_BLOCK.uid
LEFT JOIN dcp_ct2010 ON CENSUS_TRACT_BLOCK.bct2010 = boroct2010
LEFT JOIN dcp_ct2020 ON CENSUS_TRACT_BLOCK.bct2020 = boroct2020;

DROP TABLE IF EXISTS DRAFT_spatial CASCADE;
DROP TABLE IF EXISTS CENSUS_TRACT_BLOCK CASCADE;