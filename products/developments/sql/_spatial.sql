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
DROP TABLE IF EXISTS draft_spatial CASCADE;
CREATE TABLE draft_spatial AS
WITH geo_devdb_bbls AS (
    SELECT
        CASE
            WHEN
                geo_bbl IS NULL
                OR geo_bbl ~ '^0'
                OR geo_bbl = ''
                OR mode = 'tpad'
                THEN get_bbl(geom)
            ELSE geo_bbl
        END AS resolved_bbl,
        *
    FROM geo_devdb
)
SELECT DISTINCT
    id,

    -- geo_bbl
    resolved_bbl AS geo_bbl,

    -- geo_bin
    CASE
        WHEN
            resolved_bbl = get_base_bbl(geom)
            AND (
                geo_bin IS NULL
                OR geo_bin = ''
                OR geo_bin::numeric % 1000000 = 0
            )
            OR mode = 'tpad'
            THEN get_bin(geom)
        ELSE geo_bin
    END AS geo_bin,

    geo_address_numbr,
    geo_address_street,
    geo_address,

    -- geo_zipcode
    CASE
        WHEN
            geo_zipcode IS NULL
            OR geo_zipcode = ''
            OR mode = 'tpad'
            THEN get_zipcode(geom)
        ELSE geo_zipcode
    END AS geo_zipcode,

    -- geo_boro
    CASE
        WHEN
            geo_boro IS NULL
            OR geo_boro = '0'
            OR mode = 'tpad'
            THEN get_boro(geom)::text
        ELSE geo_boro
    END AS geo_boro,

    -- geo_cb2020
    CASE
        WHEN
            geo_cb2020 IS NULL
            OR geo_cb2020 = ''
            OR geo_ct2020 = '000000'
            OR mode = 'tpad'
            THEN get_cb2020(geom)
        ELSE geo_cb2020
    END AS _geo_cb2020,

    -- geo_ct2020
    CASE
        WHEN
            geo_ct2020 IS NULL
            OR geo_ct2020 = ''
            OR geo_ct2020 = '000000'
            OR mode = 'tpad'
            THEN get_ct2020(geom)
        ELSE geo_ct2020
    END AS _geo_ct2020,

    -- geo_csd
    CASE
        WHEN
            geo_csd IS NULL
            OR geo_csd = ''
            OR mode = 'tpad'
            THEN get_csd(geom)
        ELSE geo_csd
    END AS geo_csd,

    -- geo_cd
    CASE
        WHEN
            geo_cd IS NULL
            OR geo_cd = ''
            OR mode = 'tpad'
            THEN get_cd(geom)
        ELSE geo_cd
    END AS geo_cd,

    -- geo_council
    CASE
        WHEN
            geo_council IS NULL
            OR geo_council = ''
            OR mode = 'tpad'
            THEN get_council(geom)
        ELSE geo_council
    END AS geo_council,

    -- geo_policeprct
    CASE
        WHEN
            geo_policeprct IS NULL
            OR geo_policeprct = ''
            OR mode = 'tpad'
            THEN get_policeprct(geom)
        ELSE geo_policeprct
    END AS geo_policeprct,

    -- geo_firedivision
    CASE
        WHEN
            geo_firedivision IS NULL
            OR geo_firedivision = ''
            OR mode = 'tpad'
            THEN get_firedivision(geom)
        ELSE geo_firedivision
    END AS geo_firedivision,

    -- geo_firebattalion
    CASE
        WHEN
            geo_firebattalion IS NULL
            OR geo_firebattalion = ''
            OR mode = 'tpad'
            THEN get_firebattalion(geom)
        ELSE geo_firebattalion
    END AS geo_firebattalion,

    -- geo_firecompany
    CASE
        WHEN
            geo_firecompany IS NULL
            OR geo_firecompany = ''
            OR mode = 'tpad'
            THEN get_firecompany(geom)
        ELSE geo_firecompany
    END AS geo_firecompany,

    get_schoolelmntry(geom) AS geo_schoolelmntry,
    get_schoolmiddle(geom) AS geo_schoolmiddle,
    get_schoolsubdist(geom) AS geo_schoolsubdist,
    geo_latitude,
    geo_longitude,
    latitude,
    longitude,
    geom,
    geomsource
FROM geo_devdb_bbls;
DROP INDEX IF EXISTS draft_spatial_id_idx;
CREATE INDEX draft_spatial_id_idx ON draft_spatial (id);

DROP TABLE IF EXISTS census_tract_block CASCADE;
CREATE TABLE census_tract_block AS (
    SELECT DISTINCT
        id,
        CASE
            WHEN draft_spatial.geo_boro = '1' THEN '36061'
            WHEN draft_spatial.geo_boro = '2' THEN '36005'
            WHEN draft_spatial.geo_boro = '3' THEN '36047'
            WHEN draft_spatial.geo_boro = '4' THEN '36081'
            WHEN draft_spatial.geo_boro = '5' THEN '36085'
        END AS fips,
        geo_boro || _geo_ct2020 || _geo_cb2020 AS bctcb2020,
        geo_boro || _geo_ct2020 AS bct2020
    FROM draft_spatial
);

DROP INDEX IF EXISTS census_tract_block_id_idx;
DROP INDEX IF EXISTS dcp_ct2020_boroct2020_idx;
CREATE INDEX census_tract_block_id_idx ON census_tract_block (id);
CREATE INDEX dcp_ct2020_boroct2020_idx ON dcp_ct2020 (boroct2020);


DROP TABLE IF EXISTS spatial_devdb;
SELECT
    draft_spatial.id,
    draft_spatial.geo_bbl,
    draft_spatial.geo_bin,
    draft_spatial.geo_address_numbr,
    draft_spatial.geo_address_street,
    draft_spatial.geo_address,
    draft_spatial.geo_zipcode,
    draft_spatial.geo_boro,
    draft_spatial.geo_csd,
    draft_spatial.geo_cd,
    draft_spatial.geo_council,
    draft_spatial.geo_policeprct,
    draft_spatial.geo_firedivision,
    draft_spatial.geo_firebattalion,
    draft_spatial.geo_firecompany,
    draft_spatial.geo_schoolelmntry,
    draft_spatial.geo_schoolmiddle,
    draft_spatial.geo_schoolsubdist,
    draft_spatial.geo_latitude,
    draft_spatial.geo_longitude,
    draft_spatial.latitude,
    draft_spatial.longitude,
    draft_spatial.geom,
    draft_spatial.geomsource,
    census_tract_block.fips || draft_spatial._geo_ct2020 || draft_spatial._geo_cb2020 AS geo_cb2020,
    census_tract_block.fips || draft_spatial._geo_ct2020 AS geo_ct2020,
    census_tract_block.bctcb2020,
    census_tract_block.bct2020,
    dcp_ct2020.nta2020 AS geo_nta2020,
    dcp_ct2020.ntaname AS geo_ntaname2020,
    dcp_ct2020.cdta2020 AS geo_cdta2020,
    dcp_ct2020.cdtaname AS geo_cdtaname2020
INTO spatial_devdb
FROM draft_spatial
LEFT JOIN census_tract_block ON draft_spatial.id = census_tract_block.id
LEFT JOIN dcp_ct2020 ON census_tract_block.bct2020 = boroct2020;

DROP TABLE IF EXISTS draft_spatial CASCADE;
DROP TABLE IF EXISTS census_tract_block CASCADE;
