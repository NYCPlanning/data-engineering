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
DROP INDEX IF EXISTS GEO_devdb_geom_idx;
DROP TABLE IF EXISTS GEO_devdb;
WITH DRAFT AS (
    SELECT
        a.id,
        a.job_number,
		a.bbl,
        -- a.bin,
        (CASE 
            WHEN RIGHT(a.bin,6) = '000000' THEN NULL
            ELSE a.bin
        END) as bin,
        a.date_lastupdt,
        a.job_desc,
        b.geo_bbl,
        (CASE 
            WHEN RIGHT(b.geo_bin,6) = '000000' THEN NULL
            ELSE b.geo_bin
        END) as geo_bin,
        b.geo_address_numbr,
        b.geo_address_street,
        concat(
            trim(b.geo_address_numbr),' ',
            trim(b.geo_address_street)
        )as geo_address,
        b.geo_zipcode,
        COALESCE(REPLACE(b.geo_boro,'0', LEFT(b.geo_bin, 1)), a.boro) as geo_boro, 
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
        b.geo_latitude::double precision as geo_latitude,
        b.geo_longitude::double precision as geo_longitude,
        b.mode
	FROM _INIT_devdb a
	LEFT JOIN _GEO_devdb b
	ON b.id= a.id
),
GEOM_dob_bin_bldgfootprints as (
    SELECT
        a.id,
        a.job_number,
		a.bbl,
        a.bin,
        a.geo_bbl,
        a.geo_bin,
        a.geo_latitude,
        a.geo_longitude,
        ST_Centroid(b.wkb_geometry) as geom,
        (CASE WHEN b.wkb_geometry IS NOT NULL 
		 	THEN 'BIN DOB buildingfootprints' 
        END) as geomsource
    FROM DRAFT a
    LEFT JOIN doitt_buildingfootprints b
    ON a.bin::text = b.bin::numeric::bigint::text
    -- WHERE RIGHT(b.bin::text, 6) != '000000'
),
GEOM_geo_bin_bldgfootprints as (
	SELECT distinct
        a.id,
        a.job_number,
		a.bbl,
        a.bin,
        a.geo_bbl,
        a.geo_bin,
        a.geo_latitude,
        a.geo_longitude,
        coalesce(a.geom, ST_Centroid(b.wkb_geometry)) as geom,
        (CASE 
          WHEN a.geomsource IS NOT NULL 
            THEN a.geomsource 
          WHEN a.geom IS NULL 
            AND b.wkb_geometry IS NOT NULL 
            THEN 'BIN DCP geosupport'
		END) as geomsource
    FROM GEOM_dob_bin_bldgfootprints a
    LEFT JOIN doitt_buildingfootprints b
    ON a.geo_bin::text = b.bin::numeric::bigint::text
    -- WHERE RIGHT(b.bin::text, 6) != '000000'
),
GEOM_geosupport as (
    SELECT distinct
        a.id,
        a.job_number,
		a.bbl,
        a.bin,
        a.geo_bbl,
        a.geo_bin,
        coalesce(
            a.geom, 
            ST_SetSRID(ST_Point(a.geo_longitude,a.geo_latitude),4326)
        ) as geom,
        (CASE 
          WHEN a.geomsource IS NOT NULL 
            THEN a.geomsource 
          WHEN a.geom IS NULL 
            AND a.geo_longitude IS NOT NULL 
            THEN 'Lat/Lon geosupport'
		END) as geomsource
    FROM GEOM_dob_bin_bldgfootprints a
),
GEOM_dob_bbl_mappluto as (
	SELECT distinct
        a.id,
        a.job_number,
		a.bbl,
        a.bin,
        a.geo_bbl,
        coalesce(a.geom, ST_Centroid(b.wkb_geometry)) as geom,
        (CASE 
          WHEN a.geomsource IS NOT NULL 
            THEN a.geomsource 
          WHEN a.geom IS NULL 
		 		AND b.wkb_geometry IS NOT NULL 
		 		THEN 'BBL DOB MapPLUTO'
		END) as geomsource
    FROM GEOM_geosupport a
    LEFT JOIN dcp_mappluto b
    ON a.bbl = b.bbl::numeric::bigint::text
), 
buildingfootprints_historical as (
    SELECT 
        bin, 
        ST_Union(wkb_geometry) as wkb_geometry
    FROM doitt_buildingfootprints_historical
    GROUP BY bin
),
GEOM_dob_bin_bldgfp_historical as (
    SELECT distinct
        a.id,
        a.job_number,
        coalesce(a.geom, ST_Centroid(b.wkb_geometry)) as geom,
        (CASE 
		 	WHEN a.geomsource IS NOT NULL 
		 		THEN a.geomsource 
		 	WHEN a.geom IS NULL 
		 		AND b.wkb_geometry IS NOT NULL 
		 		THEN 'BIN DOB buildingfootprints (historical)'
		END) as geomsource
    FROM GEOM_dob_bbl_mappluto a
    LEFT JOIN buildingfootprints_historical b
    ON a.bin::text = b.bin::text
),
GEOM_dob_latlon as (
    SELECT distinct
        a.id,
        a.job_number,
        coalesce(
            a.geom, 
            b.dob_geom
        ) as geom,
        (CASE 
		 	WHEN a.geomsource IS NOT NULL 
		 		THEN a.geomsource 
		 	WHEN a.geom IS NULL 
		 		AND b.dob_geom IS NOT NULL 
		 		THEN 'Lat/Lon DOB'
		END) as geomsource
    FROM GEOM_dob_bin_bldgfp_historical a
    LEFT JOIN _INIT_devdb b
    ON a.job_number = b.job_number
)
SELECT
    distinct a.*,
    ST_Y(b.geom) as latitude,
    ST_X(b.geom) as longitude,
    b.geom,
    b.geomsource
INTO GEO_devdb
FROM DRAFT a
LEFT JOIN GEOM_dob_latlon b
ON a.id = b.id;

-- Create index
CREATE INDEX GEO_devdb_geom_idx ON GEO_devdb 
USING GIST (geom gist_geometry_ops_2d);
