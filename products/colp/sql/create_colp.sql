/*
DESCRIPTION:
    1) Cleans relevant columns from DCAS' IPIS.
    2) Merges these records with the results of running input BBLs
        through Geosupport's BL function.
    3) Creates category and expanded category fields for use types

        Categories are:
        -- 1: Everything else
        -- 2: Residential
        -- 3: No current use

        Expanded categories are:
        -- 1: Offices
        -- 2: Educational facilities
        -- 3: Recreational & cultural facilities, cemetaries
        -- 4: Public safety and judicial
        -- 5: Health & social services
        -- 6: Tenented & retail
        -- 7: Transportation & infrastructure
        -- 8: Not in use
        -- 9: In use residential
        -- NULL: Dispositions and other final commitments

    4) Backfills missing community districts by joining with PLUTO on the geosupport-returned
        billing BBL.

INPUTS:

    dcas_ipis (
        boro,
        block,
        lot,
        * bbl,
        house_number,
        street_name,
        parcel_name,
        agency,
        primary_use_code,
        u_a_use_cod
    )

    dcas_ipis_geocodes (
        * input_bbl,
        bill_bbl,
        longitude,
        latitude,
        x_coord,
        y_coord
    )

    dcp_mappluto (
        * bbl,
        cd
    )

OUTPUTS:
    colp (
        borough,
        block,
        lot,
        bbl,
        mapbbl,
        cd,
        hnum,
        sname,
        parcelname,
        agency,
        usecode,
        usetype,
        category,
        expandcat,
        excatdesc,
        leased,
        finalcom,
        agreement,
        xcoord,
        ycoord,
        geom
    )
*/

DROP TABLE IF EXISTS _colp CASCADE;
WITH
bbl_merge AS (
    SELECT
        b.uid,
        b.geo_bbl,
        a.*
    FROM dcas_ipis AS a
    LEFT JOIN geo_inputs AS b
        ON md5(cast((a.*) AS text)) = b.uid
),

geo_merge AS (
    SELECT
        uid,
        a.boro AS borough,
        a.block,
        a.lot,
        a.bbl,
        a.geo_bbl,
        -- Add geosupport-returned billing BBL
        (CASE
            WHEN b.bill_bbl = '0000000000' OR b.bill_bbl IS NULL
                THEN b.geo_bbl
            ELSE b.bill_bbl
        END) AS mapbbl,
        -- Include cleaned house number from source data
        CASE
            WHEN a.house_number = '0' THEN NULL
            ELSE nullif(
                ltrim(
                    ltrim(
                        regexp_replace(
                            replace(a.house_number, '&', ' AND '),
                            '[^a-zA-Z0-9 /-]+', '', 'g'
                        ),
                        '-'
                    ),
                    ' '
                ),
                ''
            )
        END AS hnum,
        -- Temporarily include source data sname
        a.street_name AS _sname,
        a.parcel_name AS _parcelname,
        (CASE
            WHEN a.agency = 'SFHZ' THEN 'PRIV'
            ELSE a.agency
        END) AS agency,
        -- Create temp use code field, without 1900 cleaning
        (lpad(split_part(cast(a.primary_use_code AS text), '.', 1), 4, '0')) AS _usecode,
        -- Create temp use type field, without 1900 cleaning
        replace(a.primary_use_text, 'STRUCTURESE', 'STRUCTURE') AS _usetype,
        -- Fill null ownership values
        (coalesce(a.owner, 'P')) AS ownership,
        nullif(a."owned/leased", 'O') AS leased,
        -- Set final commitment as D for dispositions
        (CASE
            WHEN a.u_f_use_code IS NULL OR a.u_f_use_code = ''
                THEN NULL
            ELSE 'D'
        END) AS finalcom,
        -- Parse use codes to determine agreement length
        (CASE
            WHEN
                split_part(cast(a.u_a_use_code AS text), '.', 1) = '1910'
                OR lpad(split_part(cast(a.primary_use_code AS text), '.', 1), 4, '0') = '1910'
                THEN 'L'
            WHEN
                split_part(cast(a.u_a_use_code AS text), '.', 1) = '1920'
                OR lpad(split_part(cast(a.primary_use_code AS text), '.', 1), 4, '0') = '1920'
                THEN 'S'
            WHEN
                split_part(cast(a.u_a_use_code AS text), '.', 1) = '1930'
                OR lpad(split_part(cast(a.primary_use_code AS text), '.', 1), 4, '0') = '1930'
                THEN 'M'
        END) AS agreement,
        -- Add coordinates, mappable flag, and geometry from geocode.py results
        b.x_coord AS xcoord,
        b.y_coord AS ycoord,
        b.latitude,
        b.longitude,
        (CASE
            WHEN b.longitude IS NOT NULL AND b.longitude <> ''
                THEN
                    ST_SetSRID(
                        ST_MakePoint(cast(b.longitude AS double precision), cast(b.latitude AS double precision)),
                        4326
                    )
        END) AS geom
    FROM bbl_merge AS a
    LEFT JOIN dcas_ipis_geocodes AS b
        ON a.geo_bbl = b.input_bbl
    WHERE a.owner <> 'R'
),

sname_merge AS (
    SELECT
        a.*,
        b.sname_1b AS sname
    FROM geo_merge AS a
    LEFT JOIN geo_qaqc AS b
        ON a.uid = b.uid
),

pluto_merge AS (
    SELECT
        a.*,
        -- Get CD from pluto, using donating BBL for join
        cast(b.cd AS text) AS cd
    FROM sname_merge AS a
    LEFT JOIN dcp_pluto AS b
        ON cast(cast(a.mapbbl AS numeric(19, 8)) AS text) = cast(b.bbl AS text)
),

normed_name_merge AS (
    SELECT
        a.*,
        b.new_name AS parcelname
    FROM pluto_merge AS a
    LEFT JOIN dcas_ipis_parcel_names AS b
        ON a._parcelname = b.old_name
),

categorized AS (
    SELECT
        a.*,
        -- In-use tenanted use codes should all be 1900. Length is in agreement field.
        (CASE
            WHEN a._usecode IN ('1910', '1920', '1930')
                THEN '1900'
            ELSE a._usecode
        END) AS usecode,
        -- In-use tenanted use types should all be 1900. Length is in agreement field.
        (CASE
            WHEN a._usecode IN ('1910', '1920', '1930')
                THEN 'IN USE-TENANTED'
            ELSE a._usetype
        END) AS usetype,
        -- Parse use codes to create categories
        (CASE
            WHEN a._usecode LIKE '15%' THEN '3'
            WHEN a._usecode LIKE '14%' THEN '2'
            WHEN a._usecode IS NULL OR a._usecode LIKE '16%' THEN NULL
            ELSE '1'
        END) AS category,
        -- Parse use codes to create expanded categories
        (CASE
            WHEN
                a._usecode LIKE '01%'
                OR a._usecode = '1310'
                OR a._usecode = '1340'
                OR a._usecode = '1341'
                OR a._usecode = '1349' THEN '1'
            WHEN a._usecode LIKE '02%' THEN '2'
            WHEN
                a._usecode LIKE '03%'
                OR a._usecode LIKE '04%'
                OR a._usecode = '1330' THEN '3'
            WHEN
                a._usecode LIKE '05%'
                OR a._usecode LIKE '12%'
                OR a._usecode = '1390' THEN '4'
            WHEN
                a._usecode LIKE '06%'
                OR a._usecode LIKE '07%' THEN '5'
            WHEN
                a._usecode LIKE '19%'
                OR a._usecode = '1342' THEN '6'
            WHEN
                a._usecode LIKE '08%'
                OR a._usecode LIKE '09%'
                OR a._usecode LIKE '10%'
                OR a._usecode LIKE '11%'
                OR a._usecode = '1312'
                OR a._usecode = '1313'
                OR a._usecode = '1350'
                OR a._usecode = '1360'
                OR a._usecode = '1370'
                OR a._usecode = '1380' THEN '7'
            WHEN
                a._usecode LIKE '15%'
                OR a._usecode = '1420' THEN '8'
            WHEN
                a._usecode = '1410'
                OR a._usecode = '1400' THEN '9'
        END) AS expandcat,
        -- Parse use codes to create expanded category descriptions
        (CASE
            WHEN
                a._usecode LIKE '01%'
                OR a._usecode = '1310'
                OR a._usecode = '1340'
                OR a._usecode = '1341'
                OR a._usecode = '1349' THEN 'OFFICE USE'
            WHEN a._usecode LIKE '02%' THEN 'EDUCATIONAL USE'
            WHEN
                a._usecode LIKE '03%'
                OR a._usecode LIKE '04%'
                OR a._usecode = '1330' THEN 'CULTURAL & RECREATIONAL USE'
            WHEN
                a._usecode LIKE '05%'
                OR a._usecode LIKE '12%'
                OR a._usecode = '1390' THEN 'PUBLIC SAFETY & CRIMINAL JUSTICE USE'
            WHEN
                a._usecode LIKE '06%'
                OR a._usecode LIKE '07%' THEN 'HEALTH & SOCIAL SERVICES USE'
            WHEN
                a._usecode LIKE '19%'
                OR a._usecode = '1342' THEN 'LEASED OUT TO PRIVATE TENANT'
            WHEN
                a._usecode LIKE '08%'
                OR a._usecode LIKE '09%'
                OR a._usecode LIKE '10%'
                OR a._usecode LIKE '11%'
                OR a._usecode = '1312'
                OR a._usecode = '1313'
                OR a._usecode = '1350'
                OR a._usecode = '1360'
                OR a._usecode = '1370'
                OR a._usecode = '1380' THEN 'MAINTENANCE, STORAGE, & INFRASTRUCTURE USE'
            WHEN
                a._usecode LIKE '15%'
                OR a._usecode = '1420' THEN 'PROPERTY WITH NO USE'
            WHEN
                a._usecode = '1410'
                OR a._usecode = '1400' THEN 'PROPERTY WITH RESIDENTIAL USE'
        END) AS excatdesc
    FROM normed_name_merge AS a
)

-- Reorder columns for output
SELECT DISTINCT
    uid,
    cast(geo_bbl AS numeric(19, 8)) AS geo_bbl,
    cast(borough AS varchar(2)) AS "BOROUGH",
    cast(trim(block) AS numeric(10, 0)) AS "BLOCK",
    cast(lot AS smallint) AS "LOT",
    cast(bbl AS numeric(19, 8)) AS "BBL",
    cast(mapbbl AS numeric(19, 8)) AS "MAPBBL",
    cast(cd AS smallint) AS "CD",
    cast(hnum AS varchar(20)) AS "HNUM",
    cast(sname AS varchar(40)) AS "SNAME",
    parcelname AS "PARCELNAME",
    cast(agency AS varchar(20)) AS "AGENCY",
    cast(usecode AS varchar(4)) AS "USECODE",
    usetype AS "USETYPE",
    cast(ownership AS varchar(2)) AS "OWNERSHIP",
    cast(category AS smallint) AS "CATEGORY",
    cast(expandcat AS smallint) AS "EXPANDCAT",
    excatdesc AS "EXCATDESC",
    cast(leased AS varchar(2)) AS "LEASED",
    cast(finalcom AS varchar(2)) AS "FINALCOM",
    cast(agreement AS varchar(2)) AS "AGREEMENT",
    cast(round(cast(xcoord AS numeric)) AS numeric(10, 0)) AS "XCOORD",
    cast(round(cast(ycoord AS numeric)) AS numeric(10, 0)) AS "YCOORD",
    cast(latitude AS numeric(19, 7)) AS "LATITUDE",
    cast(longitude AS numeric(19, 7)) AS "LONGITUDE",
    NULL AS "DCPEDITED",
    ST_Transform(geom, 2263) AS "GEOM"
INTO _colp
FROM categorized;
