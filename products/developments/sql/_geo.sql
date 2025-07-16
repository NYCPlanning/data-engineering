/*
DESCRIPTION:
    1. Assigning missing geoms for _GEO_devdb and create GEO_devdb
    2. Apply research corrections on (longitude, latitude, geom)

INPUTS:
    _INIT_devdb (
        * id,
        ...
    )

    _GEO_devdb (
        * id,
        ...
    )

    dcp_mappluto_wi (
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
        * id,
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
DROP INDEX IF EXISTS geo_devdb_geom_idx;
DROP TABLE IF EXISTS geo_devdb;
WITH draft AS (
    SELECT
        a.id,
        a.job_number,
        a.bbl,
        -- a.bin,
        CASE
            WHEN right(a.bin, 6) = '000000' THEN NULL
            ELSE a.bin
        END AS bin,
        a.date_lastupdt,
        a.job_desc,
        b.geo_bbl,
        CASE
            WHEN right(b.geo_bin, 6) = '000000' THEN NULL
            ELSE b.geo_bin
        END AS geo_bin,
        b.geo_address_numbr,
        b.geo_address_street,
        concat(
            trim(b.geo_address_numbr), ' ',
            trim(b.geo_address_street)
        ) AS geo_address,
        b.geo_zipcode,
        coalesce(replace(b.geo_boro, '0', left(b.geo_bin, 1)), a.boro) AS geo_boro,
        b.geo_cd,
        b.geo_council,
        b.geo_nta2020,
        b.geo_cb2020,
        b.geo_ct2020,
        b.geo_cdta2020,
        b.geo_csd,
        b.geo_policeprct,
        b.geo_firedivision,
        b.geo_firebattalion,
        b.geo_firecompany,
        b.geo_latitude::double precision AS geo_latitude,
        b.geo_longitude::double precision AS geo_longitude,
        b.mode
    FROM _init_devdb AS a
    LEFT JOIN _geo_devdb AS b
        ON a.id = b.id
),
geom_dob_bin_bldgfootprints AS (
    SELECT
        a.id,
        a.job_number,
        a.bbl,
        a.bin,
        a.geo_bbl,
        a.geo_bin,
        a.geo_latitude,
        a.geo_longitude,
        ST_Centroid(b.wkb_geometry) AS geom,
        CASE
            WHEN b.wkb_geometry IS NOT NULL THEN 'BIN DOB buildingfootprints'
        END AS geomsource
    FROM draft AS a
    LEFT JOIN doitt_buildingfootprints AS b
        ON a.bin::text = b.bin::numeric::bigint::text
-- WHERE RIGHT(b.bin::text, 6) != '000000'
),
geom_geo_bin_bldgfootprints AS (
    SELECT DISTINCT
        a.id,
        a.job_number,
        a.bbl,
        a.bin,
        a.geo_bbl,
        a.geo_bin,
        a.geo_latitude,
        a.geo_longitude,
        coalesce(a.geom, ST_Centroid(b.wkb_geometry)) AS geom,
        CASE
            WHEN a.geomsource IS NOT NULL
                THEN a.geomsource
            WHEN a.geom IS NULL AND b.wkb_geometry IS NOT NULL
                THEN 'BIN DCP geosupport'
        END AS geomsource
    FROM geom_dob_bin_bldgfootprints AS a
    LEFT JOIN doitt_buildingfootprints AS b
        ON a.geo_bin::text = b.bin::numeric::bigint::text
-- WHERE RIGHT(b.bin::text, 6) != '000000'
),
geom_geosupport AS (
    SELECT DISTINCT
        a.id,
        a.job_number,
        a.bbl,
        a.bin,
        a.geo_bbl,
        a.geo_bin,
        coalesce(
            a.geom,
            ST_SetSRID(ST_Point(a.geo_longitude, a.geo_latitude), 4326)
        ) AS geom,
        CASE
            WHEN a.geomsource IS NOT NULL
                THEN a.geomsource
            WHEN a.geom IS NULL AND a.geo_longitude IS NOT NULL
                THEN 'Lat/Lon geosupport'
        END AS geomsource
    FROM geom_dob_bin_bldgfootprints AS a
),
geom_dob_bbl_mappluto AS (
    SELECT DISTINCT
        a.id,
        a.job_number,
        a.bbl,
        a.bin,
        a.geo_bbl,
        coalesce(a.geom, ST_Centroid(b.wkb_geometry)) AS geom,
        CASE
            WHEN a.geomsource IS NOT NULL
                THEN a.geomsource
            WHEN a.geom IS NULL AND b.wkb_geometry IS NOT NULL
                THEN 'BBL DOB MapPLUTO'
        END AS geomsource
    FROM geom_geosupport AS a
    LEFT JOIN dcp_mappluto_wi AS b
        ON a.bbl = b.bbl::numeric::bigint::text
),
buildingfootprints_historical AS (
    SELECT
        bin,
        ST_Union(wkb_geometry) AS wkb_geometry
    FROM doitt_buildingfootprints_historical
    GROUP BY bin
),
geom_dob_bin_bldgfp_historical AS (
    SELECT DISTINCT
        a.id,
        a.job_number,
        coalesce(a.geom, ST_Centroid(b.wkb_geometry)) AS geom,
        CASE
            WHEN a.geomsource IS NOT NULL
                THEN a.geomsource
            WHEN a.geom IS NULL AND b.wkb_geometry IS NOT NULL
                THEN 'BIN DOB buildingfootprints (historical)'
        END AS geomsource
    FROM geom_dob_bbl_mappluto AS a
    LEFT JOIN buildingfootprints_historical AS b
        ON a.bin::text = b.bin::text
),
geom_dob_latlon AS (
    SELECT DISTINCT
        a.id,
        a.job_number,
        coalesce(
            a.geom,
            b.dob_geom
        ) AS geom,
        CASE
            WHEN a.geomsource IS NOT NULL
                THEN a.geomsource
            WHEN a.geom IS NULL AND b.dob_geom IS NOT NULL
                THEN 'Lat/Lon DOB'
        END AS geomsource
    FROM geom_dob_bin_bldgfp_historical AS a
    LEFT JOIN _init_devdb AS b
        ON a.job_number = b.job_number
)
SELECT DISTINCT
    a.*,
    ST_Y(b.geom) AS latitude,
    ST_X(b.geom) AS longitude,
    b.geom,
    b.geomsource
INTO geo_devdb
FROM draft AS a
LEFT JOIN geom_dob_latlon AS b
    ON a.id = b.id;

-- Create index
CREATE INDEX geo_devdb_geom_idx ON geo_devdb
USING gist (geom gist_geometry_ops_2d);
